package org.apache.helix.monitoring.riemann;

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

import java.util.Date;
import java.util.List;

import org.apache.helix.TestHelper;
import org.apache.helix.model.MonitoringConfig;
import org.apache.helix.monitoring.MonitoringEvent;
import org.apache.helix.monitoring.MonitoringTestHelper;
import org.apache.helix.monitoring.riemann.RawRiemannClient.State;
import org.apache.helix.monitoring.riemann.RiemannConfigs;
import org.apache.helix.monitoring.riemann.RiemannMonitoringServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.client.RiemannClient;

public class TestRiemannClientWrapper {

  @Test
  public void testBasic() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    int port = MonitoringTestHelper.availableTcpPort();

    MonitoringConfig monitoringConfig = new MonitoringConfig(RiemannConfigs.DEFAULT_RIEMANN_CONFIG);
    monitoringConfig.setConfig(MonitoringTestHelper.getRiemannConfigString(port));

    RiemannConfigs.Builder builder = new RiemannConfigs.Builder().addConfig(monitoringConfig);
    RiemannConfigs config = builder.build();
    System.out.println("configDir: " + config.getConfigDir());
    RiemannMonitoringServer server = new RiemannMonitoringServer(config);
    server.start();
    Assert.assertTrue(server.isStarted());

    // create client
    final RawRiemannClient rawRclient = new RawRiemannClient("localhost", port);
    rawRclient.connect();

    final RiemannClient rclient = RiemannClient.tcp("localhost", port);
    rclient.connect();

    MonitoringEvent event = new MonitoringEvent().tag("test").ttl(3);
    boolean ret;
    ret = rawRclient.send(event);
    Assert.assertTrue(ret);

    // wait until we can query it from riemann server
    ret = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        List<Event> events = rclient.query("tagged \"test\"");

        return events != null && events.size() == 1 && events.get(0).getTagsCount() == 1
            && events.get(0).getTags(0).equals("test");
      }
    }, 5 * 1000);
    Assert.assertTrue(ret);

    server.stop();

    // wait until heartbeat detects server is down
    ret = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        return rawRclient.getState() == State.RECONNECTING;
      }
    }, 20 * 1000);
    Assert.assertTrue(ret);

    ret = rawRclient.send(event);
    Assert.assertFalse(ret);

    server.start();
    // wait until heartbeat detects server is up again
    ret = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        return rawRclient.getState() == State.CONNECTED;
      }
    }, 20 * 1000);
    Assert.assertTrue(ret);

    MonitoringEvent event2 = new MonitoringEvent().tag("test2").ttl(3);
    ret = rawRclient.send(event2);
    Assert.assertTrue(ret);

    // wait until we can query it from riemann server
    ret = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        List<Event> events = rclient.query("tagged \"test2\"");

        return events != null && events.size() == 1 && events.get(0).getTagsCount() == 1
            && events.get(0).getTags(0).equals("test2");
      }
    }, 5 * 1000);
    Assert.assertTrue(ret);

    // clean up
    rawRclient.disconnect();
    server.stop();

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }
}
