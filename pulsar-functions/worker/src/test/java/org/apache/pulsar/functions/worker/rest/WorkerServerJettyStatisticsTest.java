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
package org.apache.pulsar.functions.worker.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.jetty.metrics.JettyStatisticsCollector;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.testng.annotations.Test;

public class WorkerServerJettyStatisticsTest {

    /** A probe collector registers only while no other Jetty collector holds the family names. */
    private static boolean jettyFamilyNamesAreFree() {
        JettyStatisticsCollector probe = new JettyStatisticsCollector(new StatisticsHandler());
        try {
            PrometheusRegistry.defaultRegistry.register(probe);
        } catch (IllegalArgumentException e) {
            return false;
        }
        PrometheusRegistry.defaultRegistry.unregister(probe);
        return true;
    }

    @Test
    public void testStopUnregistersTheJettyStatisticsCollector() throws Exception {
        // The registry is JVM-wide, so a worker server that an earlier test left running would own the families.
        assertThat(jettyFamilyNamesAreFree())
                .as("no Jetty collector is registered before the worker server is created").isTrue();
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerPort(0);
        WorkerService workerService = mock(WorkerService.class);
        when(workerService.getWorkerConfig()).thenReturn(workerConfig);
        // The constructor registers the collector.
        WorkerServer workerServer = new WorkerServer(workerService, mock(AuthenticationService.class));
        try {
            workerServer.start();
            assertThat(jettyFamilyNamesAreFree()).as("the running worker server holds the Jetty families").isFalse();
        } finally {
            workerServer.stop();
        }
        assertThat(jettyFamilyNamesAreFree())
                .as("stop() unregisters the Jetty collector that the worker server registered").isTrue();
    }
}
