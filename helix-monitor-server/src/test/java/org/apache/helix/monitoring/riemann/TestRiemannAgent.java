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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.MonitoringConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.monitoring.MonitoringTestHelper;
import org.apache.helix.monitoring.riemann.RiemannAgent;
import org.apache.helix.monitoring.riemann.RiemannMonitoringServer;
import org.apache.helix.monitoring.riemann.RiemannConfigs.Builder;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRiemannAgent extends ZkUnitTestBase {
  @Test
  public void testStartAndStop() throws Exception {
    final int NUM_PARTICIPANTS = 0;
    final int NUM_PARTITIONS = 4;
    final int NUM_REPLICAS = 1;

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up monitoring cluster
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "MonitoringService", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "OnlineOffline", // pick a built-in state model
        RebalanceMode.FULL_AUTO, // let Helix handle rebalancing
        true); // do rebalance

    // Enable auto-join
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    configAccessor.set(scope, ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "" + true);

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // Start monitoring server
    int port = MonitoringTestHelper.availableTcpPort();
    RiemannMonitoringServer server = MonitoringTestHelper.startRiemannServer(port);

    // Start Riemann agent
    RiemannAgent agent = new RiemannAgent(ZK_ADDR, clusterName, port);
    agent.start();

    // Check live-instance
    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<String> liveInstances = accessor.getChildNames(keyBuilder.liveInstances());
    Assert.assertNotNull(liveInstances);
    Assert.assertEquals(liveInstances.size(), 1);

    // Stop monitoring server
    server.stop();

    boolean result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        List<String> liveInstances = accessor.getChildNames(keyBuilder.liveInstances());
        return liveInstances != null && liveInstances.size() == 0;
      }
    }, 15 * 1000);
    Assert.assertTrue(result, "RiemannAgent should be disconnected if RiemannServer is stopped");

    agent.shutdown();
    controller.syncStop();

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
