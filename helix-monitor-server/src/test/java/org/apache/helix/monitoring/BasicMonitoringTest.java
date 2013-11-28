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

import java.net.InetAddress;
import java.util.Date;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Leader;
import org.junit.Assert;
import org.testng.annotations.Test;

public class BasicMonitoringTest extends ZkUnitTestBase {
  @Test
  public void testStartAndStop() throws Exception {
    final int NUM_PARTICIPANTS = 10;
    final int NUM_PARTITIONS = 4;
    final int NUM_REPLICAS = 2;

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "MasterSlave", // pick a built-in state model
        RebalanceMode.FULL_AUTO, // let Helix handle rebalancing
        true); // do rebalance

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.registerMonitoringServer(new RiemannMonitoringServer(InetAddress.getLocalHost()
        .getHostName()));
    controller.syncStart();

    // make sure the leader has registered and is showing the server port
    HelixDataAccessor accessor = controller.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Leader leader = accessor.getProperty(keyBuilder.controllerLeader());
    Assert.assertNotNull(leader);
    Assert.assertNotEquals(leader.getMonitoringPort(), -1);
    Assert.assertNotNull(leader.getMonitoringHost());

    // stop controller
    controller.syncStop();

    // start controller without monitoring
    ClusterControllerManager rawController =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    rawController.syncStart();

    // make sure the leader has registered, but has no monitoring port
    accessor = rawController.getHelixDataAccessor();
    keyBuilder = accessor.keyBuilder();
    leader = accessor.getProperty(keyBuilder.controllerLeader());
    Assert.assertNotNull(leader);
    Assert.assertEquals(leader.getMonitoringPort(), -1);
    Assert.assertNull(leader.getMonitoringHost());

    // stop controller
    rawController.syncStop();
  }
}
