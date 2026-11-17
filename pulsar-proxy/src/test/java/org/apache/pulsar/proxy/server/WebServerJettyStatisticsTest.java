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
package org.apache.pulsar.proxy.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.Optional;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.jetty.metrics.JettyStatisticsCollector;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.testng.annotations.Test;

@Test(groups = "proxy")
public class WebServerJettyStatisticsTest {

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
        // The registry is JVM-wide, so a web server that an earlier test left running would own the families.
        assertThat(jettyFamilyNamesAreFree())
                .as("no Jetty collector is registered before the web server starts").isTrue();
        ProxyConfiguration config = new ProxyConfiguration();
        config.setWebServicePort(Optional.of(0));
        WebServer webServer = new WebServer(config, mock(AuthenticationService.class));
        try {
            webServer.start();
            assertThat(jettyFamilyNamesAreFree()).as("the running web server holds the Jetty families").isFalse();
        } finally {
            webServer.stop();
        }
        assertThat(jettyFamilyNamesAreFree())
                .as("stop() unregisters the Jetty collector that the web server registered").isTrue();
    }
}
