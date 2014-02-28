package org.apache.helix.monitoring;

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

import java.io.IOException;
import java.util.Date;

import org.apache.helix.MonitoringTestHelper;
import org.apache.helix.TestHelper;
import org.apache.helix.model.MonitoringConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.aphyr.riemann.client.RiemannClient;

public class TestRiemannMonitoringServer {

  @Test
  public void testBasic() throws IOException {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    int port = MonitoringTestHelper.availableTcpPort();
    MonitoringConfig monitoringConfig = new MonitoringConfig(RiemannConfigs.DEFAULT_RIEMANN_CONFIG);
    monitoringConfig.setConfig(MonitoringTestHelper.getRiemannConfigString(port));

    RiemannConfigs.Builder builder = new RiemannConfigs.Builder().addConfig(monitoringConfig);
    RiemannMonitoringServer server = new RiemannMonitoringServer(builder.build());

    // Check server starts
    server.start();
    Assert.assertTrue(server.isStarted());

    RiemannClient rclient = null;
    try {
      rclient = RiemannClient.tcp("localhost", port);
      rclient.connect();
    } catch (IOException e) {
      Assert.fail("Riemann server should start on port: " + port);
    }

    // Check server stops
    Assert.assertNotNull(rclient);
    rclient.disconnect();
    server.stop();
    Assert.assertFalse(server.isStarted());

    try {
      rclient = RiemannClient.tcp("localhost", port);
      rclient.connect();
      Assert.fail("Riemann server should be stopped on port: " + port);
    } catch (IOException e) {
      // ok
    }

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }
}
