/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.stats.prometheus;

import io.netty.buffer.ByteBuf;
import io.prometheus.metrics.config.EscapingScheme;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * Generate metrics in a text format suitable to be consumed by Prometheus.
 * Format specification can be found at <a href="https://prometheus.io/docs/instrumenting/exposition_formats/">exposition formats</a>
 */
public class PrometheusMetricsGeneratorUtils {

    private static final PrometheusTextFormatWriter TEXT_WRITER = PrometheusTextFormatWriter.builder()
            .setIncludeCreatedTimestamps(false).build();

    public static void generate(String cluster, OutputStream out,
                                List<PrometheusRawMetricsProvider> metricsProviders)
            throws IOException {
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.heapBuffer();
        try {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);
            generateSystemMetrics(stream, cluster);
            if (metricsProviders != null) {
                for (PrometheusRawMetricsProvider metricsProvider : metricsProviders) {
                    metricsProvider.generate(stream);
                }
            }
            out.write(buf.array(), buf.arrayOffset(), buf.readableBytes());
        } finally {
            buf.release();
        }
    }

    public static void generateSystemMetrics(SimpleTextOutputStream stream, String cluster) {
        generateSystemMetrics(stream, cluster, PrometheusRegistry.defaultRegistry);
    }

    static void generateSystemMetrics(SimpleTextOutputStream stream, String cluster, PrometheusRegistry registry) {
        MetricSnapshots snapshots = registry.scrape();
        if (snapshots.size() == 0) {
            return;
        }
        try {
            TEXT_WRITER.write(new PulsarPrometheusTextOutput(stream, cluster), snapshots,
                    EscapingScheme.UNDERSCORE_ESCAPING);
        } catch (IOException e) {
            // The destination is an in-memory stream; keep the existing unchecked scrape API.
            throw new UncheckedIOException(e);
        }
    }

}
