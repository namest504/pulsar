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

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.prometheus.metrics.model.registry.Collector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.ClassicHistogramBuckets;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.InfoSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.NativeHistogramBuckets;
import io.prometheus.metrics.model.snapshots.Quantile;
import io.prometheus.metrics.model.snapshots.Quantiles;
import io.prometheus.metrics.model.snapshots.StateSetSnapshot;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import io.prometheus.metrics.model.snapshots.Unit;
import io.prometheus.metrics.model.snapshots.UnknownSnapshot;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PrometheusMetricsGeneratorUtilsTest {

    private static final String LABEL_NAME_CLUSTER = "cluster";

    @Test
    public void testAllMetricTypesAreWrittenWithTheClusterLabel() {
        PrometheusRegistry registry = new PrometheusRegistry();
        registry.register(() -> CounterSnapshot.builder().name("requests")
                .dataPoint(CounterSnapshot.CounterDataPointSnapshot.builder()
                        .labels(Labels.of("result", "ok")).value(3).build())
                .dataPoint(CounterSnapshot.CounterDataPointSnapshot.builder()
                        .labels(Labels.of(LABEL_NAME_CLUSTER, "own")).value(1).build())
                .build());
        registry.register(() -> GaugeSnapshot.builder().name("queue.size_bytes").unit(Unit.BYTES)
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .labels(Labels.of("queue", "a\"b\\c\nd")).value(2.5).build())
                .build());
        registry.register(() -> HistogramSnapshot.builder().name("latency")
                .dataPoint(HistogramSnapshot.HistogramDataPointSnapshot.builder()
                        .classicHistogramBuckets(ClassicHistogramBuckets.of(
                                new double[]{1, 2, Double.POSITIVE_INFINITY}, new long[]{1, 1, 1}))
                        .sum(4.5).build())
                .build());
        registry.register(() -> SummarySnapshot.builder().name("rpc")
                .dataPoint(SummarySnapshot.SummaryDataPointSnapshot.builder()
                        .quantiles(Quantiles.of(new Quantile(0.5, 2))).count(2).sum(8).build())
                .build());
        registry.register(() -> InfoSnapshot.builder().name("build")
                .dataPoint(InfoSnapshot.InfoDataPointSnapshot.builder()
                        .labels(Labels.of("version", "4.2")).build())
                .build());
        registry.register(() -> StateSetSnapshot.builder().name("mode")
                .dataPoint(StateSetSnapshot.StateSetDataPointSnapshot.builder()
                        .state("active", true).state("standby", false).build())
                .build());
        registry.register(() -> UnknownSnapshot.builder().name("legacy_value")
                .dataPoint(UnknownSnapshot.UnknownDataPointSnapshot.builder().value(1.5).build())
                .build());

        // The 1.x registry scrapes its families sorted by name and their data points sorted by labels.
        assertThat(render("c1", registry)).isEqualTo("""
                # TYPE build_info gauge
                build_info{cluster="c1",version="4.2"} 1
                # TYPE latency histogram
                latency_bucket{cluster="c1",le="1.0"} 1
                latency_bucket{cluster="c1",le="2.0"} 2
                latency_bucket{cluster="c1",le="+Inf"} 3
                latency_count{cluster="c1"} 3
                latency_sum{cluster="c1"} 4.5
                # TYPE legacy_value untyped
                legacy_value{cluster="c1"} 1.5
                # TYPE mode gauge
                mode{cluster="c1",mode="active"} 1
                mode{cluster="c1",mode="standby"} 0
                # TYPE queue_size_bytes gauge
                queue_size_bytes{cluster="c1",queue="a\\"b\\\\c\\nd"} 2.5
                # TYPE requests_total counter
                requests_total{cluster="own"} 1.0
                requests_total{cluster="c1",result="ok"} 3.0
                # TYPE rpc summary
                rpc{cluster="c1",quantile="0.5"} 2.0
                rpc_count{cluster="c1"} 2
                rpc_sum{cluster="c1"} 8.0
                """);
    }

    @Test
    public void testGaugeHistogramUsesGaugeCountAndSum() {
        PrometheusRegistry registry = new PrometheusRegistry();
        registry.register(() -> HistogramSnapshot.builder().name("inflight").gaugeHistogram(true)
                .dataPoint(HistogramSnapshot.HistogramDataPointSnapshot.builder()
                        .labels(Labels.of("q", "a"))
                        .classicHistogramBuckets(ClassicHistogramBuckets.of(
                                new double[]{1, Double.POSITIVE_INFINITY}, new long[]{2, 0}))
                        .sum(1.5).build())
                .dataPoint(HistogramSnapshot.HistogramDataPointSnapshot.builder()
                        .labels(Labels.of("q", "b"))
                        .classicHistogramBuckets(ClassicHistogramBuckets.of(
                                new double[]{1, Double.POSITIVE_INFINITY}, new long[]{0, 1}))
                        .sum(3).build())
                .build());

        // _gcount and _gsum are not part of the text format's histogram; the 1.x writer declares them as gauges.
        assertThat(render("c1", registry)).isEqualTo("""
                # TYPE inflight histogram
                inflight_bucket{cluster="c1",q="a",le="1.0"} 2
                inflight_bucket{cluster="c1",q="a",le="+Inf"} 2
                inflight_bucket{cluster="c1",q="b",le="1.0"} 0
                inflight_bucket{cluster="c1",q="b",le="+Inf"} 1
                # TYPE inflight_gcount gauge
                inflight_gcount{cluster="c1",q="a"} 2
                inflight_gcount{cluster="c1",q="b"} 1
                # TYPE inflight_gsum gauge
                inflight_gsum{cluster="c1",q="a"} 1.5
                inflight_gsum{cluster="c1",q="b"} 3.0
                """);
    }

    @Test
    public void testNativeOnlyHistogramIsExposedAsSingleBucket() {
        PrometheusRegistry registry = new PrometheusRegistry();
        registry.register(() -> HistogramSnapshot.builder().name("native")
                .dataPoint(HistogramSnapshot.HistogramDataPointSnapshot.builder()
                        .nativeSchema(5).nativeZeroCount(1)
                        .nativeBucketsForPositiveValues(NativeHistogramBuckets.of(new int[]{0}, new long[]{3}))
                        .sum(9).build())
                .build());

        assertThat(render("c1", registry)).isEqualTo("""
                # TYPE native histogram
                native_bucket{cluster="c1",le="+Inf"} 4
                native_count{cluster="c1"} 4
                native_sum{cluster="c1"} 9.0
                """);
    }


    @Test
    public void testCounterAndInfoNamesKeepTheirSuffix() {
        PrometheusRegistry registry = new PrometheusRegistry();
        registry.register(() -> CounterSnapshot.builder().name("already_total")
                .dataPoint(CounterSnapshot.CounterDataPointSnapshot.builder().value(1).build())
                .build());
        registry.register(() -> InfoSnapshot.builder().name("target_info")
                .dataPoint(InfoSnapshot.InfoDataPointSnapshot.builder().labels(Labels.of("v", "1")).build())
                .build());

        assertThat(render("c1", registry)).isEqualTo("""
                # TYPE already_total counter
                already_total{cluster="c1"} 1.0
                # TYPE target_info gauge
                target_info{cluster="c1",v="1"} 1
                """);
    }


    @Test
    public void testEscapedUnicodeLabelsAndExistingCluster() {
        PrometheusRegistry registry = new PrometheusRegistry();
        // Longer than the official writer's buffer, with a supplementary character next to an escape.
        String value = "x".repeat(9000) + "\uD834\uDD1E\\\"\n,cluster=\"not-a-label\"}";
        registry.register(() -> GaugeSnapshot.builder().name("escaped").help("help\ntext")
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .labels(Labels.of("detail", value)).value(1).build())
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .labels(Labels.of("cluster", "own", "detail", value)).value(2).build())
                .build());
        String escaped = value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
        assertThat(render("c\\\"\n\uD834\uDD1E", registry)).isEqualTo(
                "# TYPE escaped gauge\n"
                        + "escaped{cluster=\"own\",detail=\"" + escaped + "\"} 2.0\n"
                        + "escaped{cluster=\"c\\\\\\\"\\n\uD834\uDD1E\",detail=\"" + escaped + "\"} 1.0\n");
    }

    @Test
    public void testClusterLabelDetectionAndScrapeTimestamp() {
        PrometheusRegistry registry = new PrometheusRegistry();
        registry.register(() -> GaugeSnapshot.builder().name("g")
                // Sorted after another label, so it is not the first label in the braces.
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .labels(Labels.of("a", "1", "cluster", "own")).value(1).build())
                // Names that merely contain "cluster" still get the cluster label injected.
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .labels(Labels.of("cluster_id", "x")).value(2).build())
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .labels(Labels.of("my_cluster", "x")).value(3).build())
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .labels(Labels.of("clusters", "x")).value(4).build())
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .scrapeTimestampMillis(1700000000123L).value(5).build())
                .build());

        // Data points come out in the client's label order.
        assertThat(render("c1", registry)).isEqualTo("""
                # TYPE g gauge
                g{cluster="c1"} 5.0 1700000000123
                g{a="1",cluster="own"} 1.0
                g{cluster="c1",cluster_id="x"} 2.0
                g{cluster="c1",clusters="x"} 4.0
                g{cluster="c1",my_cluster="x"} 3.0
                """);
    }

    @Test
    public void testGenerateIncludesDefaultPrometheusRegistry() throws Exception {
        String metricsName = "prometheus_registry_" + randomString();
        MetricSnapshot snapshot = CounterSnapshot.builder().name(metricsName)
                .dataPoint(CounterSnapshot.CounterDataPointSnapshot.builder().value(2).build())
                .build();
        Collector collector = () -> snapshot;
        PrometheusRegistry.defaultRegistry.register(collector);
        try {
            assertThat(exportMetrics())
                    .contains("# TYPE " + metricsName + "_total counter\n"
                            + metricsName + "_total{cluster=\"compatibility\"} 2.0\n");
        } finally {
            PrometheusRegistry.defaultRegistry.unregister(collector);
        }
        assertThat(exportMetrics()).doesNotContain(metricsName);
    }

    private static String render(String cluster, PrometheusRegistry registry) {
        ByteBuf buf = Unpooled.buffer();
        try {
            PrometheusMetricsGeneratorUtils.generateSystemMetrics(new SimpleTextOutputStream(buf), cluster, registry);
            return buf.toString(StandardCharsets.UTF_8);
        } finally {
            buf.release();
        }
    }

    private static String exportMetrics() throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            PrometheusMetricsGeneratorUtils.generate("compatibility", out, Collections.emptyList());
            return out.toString(StandardCharsets.UTF_8);
        }
    }

    private static String randomString(){
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
}
