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
package org.apache.pulsar.metadata.impl.stats;

import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.ClassicHistogramBucket;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot.HistogramDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

@Test(groups = "metadata")
public class MetadataStoreStatsCollectorsTest {

    private static final String OPS_LATENCY = "pulsar_metadata_store_ops_latency_ms";
    private static final String PUT_BYTES = "pulsar_metadata_store_put_bytes";
    private static final String QUEUE_WAIT_TIME = "pulsar_batch_metadata_store_queue_wait_time_ms";
    private static final String BATCH_EXECUTE_TIME = "pulsar_batch_metadata_store_batch_execute_time_ms";
    private static final String BATCH_SIZE = "pulsar_batch_metadata_store_batch_size";
    private static final String NAME_LABEL = "name";

    private static final List<Double> OPS_LATENCY_BOUNDS = List.of(
            1.0, 3.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0,
            Double.POSITIVE_INFINITY);
    private static final List<Double> BATCH_BOUNDS = List.of(
            1.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, Double.POSITIVE_INFINITY);

    private static String uniqueStoreName() {
        return "stats-test-" + UUID.randomUUID();
    }

    private static Set<Labels> labelsOfStore(String metricName, String storeName) {
        return PrometheusRegistry.defaultRegistry.scrape(metricName::equals).stream()
                .flatMap(snapshot -> snapshot.getDataPoints().stream())
                .map(DataPointSnapshot::getLabels)
                .filter(labels -> storeName.equals(labels.get(NAME_LABEL)))
                .collect(Collectors.toSet());
    }

    private static void assertClassicOnlyBuckets(String metricName, String storeName, List<Double> expectedBounds) {
        List<HistogramDataPointSnapshot> dataPoints = PrometheusRegistry.defaultRegistry.scrape(metricName::equals)
                .stream()
                .flatMap(snapshot -> ((HistogramSnapshot) snapshot).getDataPoints().stream())
                .filter(dataPoint -> storeName.equals(dataPoint.getLabels().get(NAME_LABEL)))
                .toList();
        assertThat(dataPoints).as("data points of %s for store %s", metricName, storeName).isNotEmpty();
        for (HistogramDataPointSnapshot dataPoint : dataPoints) {
            assertThat(dataPoint.getClassicBuckets().stream().map(ClassicHistogramBucket::getUpperBound).toList())
                    .as("classic bucket upper bounds of %s%s", metricName, dataPoint.getLabels())
                    .isEqualTo(expectedBounds);
            assertThat(dataPoint.hasNativeHistogramData())
                    .as("%s%s has native histogram data", metricName, dataPoint.getLabels())
                    .isFalse();
        }
    }

    @Test
    public void testOpsLatencyHistogramBucketsAreClassicOnly() throws Exception {
        String storeName = uniqueStoreName();
        MetadataStoreStats stats = new MetadataStoreStats(storeName, OpenTelemetry.noop());
        try {
            stats.recordGetOpsSucceeded(7);
            assertClassicOnlyBuckets(OPS_LATENCY, storeName, OPS_LATENCY_BOUNDS);
        } finally {
            stats.close();
        }
    }

    @Test
    public void testBatchHistogramsBucketsAreClassicOnly() throws Exception {
        String storeName = uniqueStoreName();
        BatchMetadataStoreStats stats = new BatchMetadataStoreStats(storeName);
        try {
            stats.recordOpWaiting(7);
            stats.recordBatchExecuteTime(7);
            stats.recordOpsInBatch(7);
            assertClassicOnlyBuckets(QUEUE_WAIT_TIME, storeName, BATCH_BOUNDS);
            assertClassicOnlyBuckets(BATCH_EXECUTE_TIME, storeName, BATCH_BOUNDS);
            assertClassicOnlyBuckets(BATCH_SIZE, storeName, BATCH_BOUNDS);
        } finally {
            stats.close();
        }
    }

    @Test
    public void testCloseRemovesMetadataStoreSeries() throws Exception {
        String storeName = uniqueStoreName();
        MetadataStoreStats stats = new MetadataStoreStats(storeName, OpenTelemetry.noop());
        try {
            stats.recordGetOpsSucceeded(1);
            stats.recordDelOpsSucceeded(1);
            stats.recordPutOpsSucceeded(1, 10);
            stats.recordGetOpsFailed(1);
            stats.recordDelOpsFailed(1);
            stats.recordPutOpsFailed(1);
            assertThat(labelsOfStore(OPS_LATENCY, storeName))
                    .as("%s series before close", OPS_LATENCY)
                    .hasSize(6);
            assertThat(labelsOfStore(PUT_BYTES, storeName))
                    .as("%s series before close", PUT_BYTES)
                    .hasSize(1);
        } finally {
            stats.close();
        }
        assertThat(labelsOfStore(OPS_LATENCY, storeName))
                .as("%s series left after close", OPS_LATENCY)
                .isEqualTo(Set.of());
        assertThat(labelsOfStore(PUT_BYTES, storeName))
                .as("%s series left after close", PUT_BYTES)
                .isEqualTo(Set.of());
    }

    @Test
    public void testCloseRemovesBatchMetadataStoreSeries() throws Exception {
        String storeName = uniqueStoreName();
        BatchMetadataStoreStats stats = new BatchMetadataStoreStats(storeName);
        try {
            stats.recordOpWaiting(1);
            stats.recordBatchExecuteTime(1);
            stats.recordOpsInBatch(1);
            for (String metricName : List.of(QUEUE_WAIT_TIME, BATCH_EXECUTE_TIME, BATCH_SIZE)) {
                assertThat(labelsOfStore(metricName, storeName))
                        .as("%s series before close", metricName)
                        .hasSize(1);
            }
        } finally {
            stats.close();
        }
        for (String metricName : List.of(QUEUE_WAIT_TIME, BATCH_EXECUTE_TIME, BATCH_SIZE)) {
            assertThat(labelsOfStore(metricName, storeName))
                    .as("%s series left after close", metricName)
                    .isEqualTo(Set.of());
        }
    }
}
