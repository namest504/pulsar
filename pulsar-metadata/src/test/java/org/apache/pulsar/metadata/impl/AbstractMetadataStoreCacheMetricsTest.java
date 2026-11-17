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
package org.apache.pulsar.metadata.impl;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.core.type.TypeReference;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.cache.impl.JSONMetadataSerdeTypeRef;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "metadata")
public class AbstractMetadataStoreCacheMetricsTest {
    private static final String NAMED_CACHE_SUFFIX = "-named";

    private String storeName;
    private String childrenCacheName;
    private String existsCacheName;
    private String metadataCacheName;

    @BeforeMethod
    public void setUp() {
        storeName = "cache-metrics-" + UUID.randomUUID();
        childrenCacheName = storeName + "-children";
        existsCacheName = storeName + "-exists";
        metadataCacheName = storeName;
    }

    @AfterMethod(alwaysRun = true)
    public void removeCacheMetricsRegisteredByThisTest() {
        CacheMetricsCollector.CAFFEINE.removeCache(childrenCacheName);
        CacheMetricsCollector.CAFFEINE.removeCache(existsCacheName);
        CacheMetricsCollector.CAFFEINE.removeCache(metadataCacheName);
        CacheMetricsCollector.CAFFEINE.removeCache(storeName + NAMED_CACHE_SUFFIX);
    }

    private interface MetadataCacheFactory {
        MetadataCache<String> create(LocalMemoryMetadataStore store, String cacheName);
    }

    @DataProvider
    public Object[][] metadataCacheOverloads() {
        MetadataCacheFactory byClass = (store, cacheName) -> store.getMetadataCache(String.class);
        MetadataCacheFactory byTypeReference =
                (store, cacheName) -> store.getMetadataCache(new TypeReference<String>() {});
        MetadataCacheFactory byNamedSerde = (store, cacheName) -> store.getMetadataCache(cacheName,
                new JSONMetadataSerdeTypeRef<>(new TypeReference<String>() {}),
                store.getDefaultMetadataCacheConfig());
        return new Object[][] {
                {"Class", "", byClass},
                {"TypeReference", "", byTypeReference},
                {"named MetadataSerde", NAMED_CACHE_SUFFIX, byNamedSerde},
        };
    }

    @Test
    public void testCloseRemovesChildrenAndExistsCacheMetrics() throws Exception {
        LocalMemoryMetadataStore store = newStoreNamed(storeName);
        try {
            assertThat(exposedCacheNames()).as("cache series exposed while the store is open")
                    .contains(childrenCacheName, existsCacheName);
        } finally {
            store.close();
        }

        assertThat(exposedCacheNames()).as("cache series exposed after the store is closed")
                .doesNotContain(childrenCacheName, existsCacheName);

        store.close();
        assertThat(exposedCacheNames()).as("cache series exposed after closing the store twice")
                .doesNotContain(childrenCacheName, existsCacheName);
    }

    @Test
    public void testCloseKeepsMetricsOfLiveStoreWithSameName() throws Exception {
        LocalMemoryMetadataStore oldStore = newStoreNamed(storeName);
        LocalMemoryMetadataStore liveStore = newStoreNamed(storeName);
        try {
            oldStore.close();
            assertThat(exposedCacheNames()).as("cache series of the live store after closing the old store")
                    .contains(childrenCacheName, existsCacheName);
        } finally {
            liveStore.close();
        }

        assertThat(exposedCacheNames()).as("cache series exposed after both stores are closed")
                .doesNotContain(childrenCacheName, existsCacheName);
    }

    @Test(dataProvider = "metadataCacheOverloads")
    public void testCloseRemovesMetadataCacheMetrics(String overload, String cacheNameSuffix,
                                                     MetadataCacheFactory factory) throws Exception {
        String cacheName = storeName + cacheNameSuffix;
        LocalMemoryMetadataStore store = newStoreNamed(storeName);
        try {
            factory.create(store, cacheName);
            assertThat(exposedCacheNames()).as("metadata cache series exposed while the store is open")
                    .contains(cacheName);
        } finally {
            store.close();
        }

        assertThat(exposedCacheNames()).as("metadata cache series exposed after the store is closed")
                .doesNotContain(cacheName);
    }

    @Test
    public void testCloseKeepsValuesCachedInMetadataCache() throws Exception {
        String path = "/cached-value";
        LocalMemoryMetadataStore store = newStoreNamed(storeName);
        MetadataCache<String> cache;
        try {
            cache = store.getMetadataCache(String.class);
            CompletableFuture<Void> putNotificationHandled = new CompletableFuture<>();
            store.registerCancellableListener(notification -> {
                if (notification.getPath().equals(path)) {
                    putNotificationHandled.complete(null);
                }
            });
            store.put(path, "\"value\"".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
            // The store refreshes a cached value when it handles the notification of the put. After close() that
            // refresh would fail and drop the value, so the value is cached once the notification is handled.
            putNotificationHandled.get(10, TimeUnit.SECONDS);
            assertThat(cache.get(path).join()).as("value loaded through the metadata cache")
                    .contains("value");
        } finally {
            store.close();
        }

        assertThat(cache.getIfCached(path)).as("a value cached before the store was closed")
                .contains("value");
    }

    @Test
    public void testCloseKeepsMetadataCacheMetricsOfLiveStoreWithSameName() throws Exception {
        LocalMemoryMetadataStore oldStore = newStoreNamed(storeName);
        oldStore.getMetadataCache(String.class);
        LocalMemoryMetadataStore liveStore = newStoreNamed(storeName);
        try {
            liveStore.getMetadataCache(String.class);
            oldStore.close();
            assertThat(exposedCacheNames())
                    .as("metadata cache series of the live store after closing the old store")
                    .contains(metadataCacheName);
        } finally {
            liveStore.close();
        }

        assertThat(exposedCacheNames()).as("metadata cache series exposed after both stores are closed")
                .doesNotContain(metadataCacheName);
    }

    @Test(dataProvider = "metadataCacheOverloads")
    public void testMetadataCacheCreatedAfterCloseLeavesNoMetrics(String overload, String cacheNameSuffix,
                                                                 MetadataCacheFactory factory) throws Exception {
        String cacheName = storeName + cacheNameSuffix;
        LocalMemoryMetadataStore closedStore = newStoreNamed(storeName);
        closedStore.close();

        factory.create(closedStore, cacheName);
        assertThat(exposedCacheNames()).as("series of a metadata cache created after the store is closed")
                .doesNotContain(cacheName);
    }

    private static LocalMemoryMetadataStore newStoreNamed(String name) throws Exception {
        return new LocalMemoryMetadataStore("memory:local",
                MetadataStoreConfig.builder().metadataStoreName(name).build());
    }

    private static Set<String> exposedCacheNames() {
        Set<String> cacheNames = new HashSet<>();
        for (MetricSnapshot snapshot : PrometheusRegistry.defaultRegistry.scrape()) {
            if (!snapshot.getMetadata().getPrometheusName().startsWith("caffeine_cache_")) {
                continue;
            }
            for (DataPointSnapshot dataPoint : snapshot.getDataPoints()) {
                String cacheName = dataPoint.getLabels().get("cache");
                if (cacheName != null) {
                    cacheNames.add(cacheName);
                }
            }
        }
        return cacheNames;
    }
}
