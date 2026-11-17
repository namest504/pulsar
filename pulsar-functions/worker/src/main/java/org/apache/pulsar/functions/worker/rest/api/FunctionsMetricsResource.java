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
package org.apache.pulsar.functions.worker.rest.api;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsServlet;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;

@Path("/")
public class FunctionsMetricsResource extends FunctionApiResource {
    @Path("metrics")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getMetrics() throws IOException {

        WorkerService workerService = get();
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.heapBuffer();
        // if request, also attach the prometheus metrics
        if (workerService.getWorkerConfig().isIncludeStandardPrometheusMetrics()) {
            PrometheusTextFormatWriter.create().write(new ByteBufOutputStream(buf),
                    PrometheusRegistry.defaultRegistry.scrape());
        }

        try {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);
            workerService.generateFunctionsStats(stream);
            byte[] payload = buf.array();
            int arrayOffset = buf.arrayOffset();
            int readableBytes = buf.readableBytes();
            StreamingOutput streamOut = out -> {
                out.write(payload, arrayOffset, readableBytes);
                out.flush();
            };
            return Response
                .ok(streamOut)
                .type(PrometheusMetricsServlet.PROMETHEUS_CONTENT_TYPE_004)
                .build();
        } finally {
            buf.release();
        }
    }
}
