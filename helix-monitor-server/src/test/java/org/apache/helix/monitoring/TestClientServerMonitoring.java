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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Leader;
import org.apache.helix.model.MonitoringConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class TestClientServerMonitoring extends ZkUnitTestBase {
  @Test
  public void testMonitoring() throws Exception {
    final int NUM_PARTICIPANTS = 4;
    final int NUM_PARTITIONS = 8;
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

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      participants[i] =
          new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      participants[i].syncStart();
    }
    HelixDataAccessor accessor = participants[0].getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // set a custom monitoring config
    MonitoringConfig monitoringConfig = new MonitoringConfig("sampleMonitoringConfig");
    monitoringConfig.setConfig(getMonitoringConfigString());
    accessor.setProperty(keyBuilder.monitoringConfig("sampleMonitoringConfig"), monitoringConfig);

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.registerMonitoringServer(new RiemannMonitoringServer(InetAddress.getLocalHost()
        .getHostName()));
    controller.syncStart();

    // make sure the leader has registered and is showing the server port
    Leader leader = accessor.getProperty(keyBuilder.controllerLeader());
    Assert.assertNotNull(leader);
    Assert.assertNotEquals(leader.getMonitoringPort(), -1);
    Assert.assertNotNull(leader.getMonitoringHost());

    // run the spectator
    spectate(clusterName, "TestDB0", NUM_PARTITIONS);

    // stop participants
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }

    // stop controller
    controller.syncStop();
  }

  private String getMonitoringConfigString() {
    StringBuilder sb =
        new StringBuilder()
            .append("(defn parse-int\r\n")
            .append(
                "  \"Convert a string to an integer\"\r\n  [instr]\r\n  (Integer/parseInt instr))\r\n\r\n")
            .append("(defn parse-double\r\n  \"Convert a string into a double\"\r\n  [instr]\r\n")
            .append("  (Double/parseDouble instr))\r\n\r\n(defn check-failure-rate\r\n")
            .append(
                "  \"Check if the event should trigger an alarm based on failure rate\"\r\n  [e]\r\n")
            .append(
                "  (let [writeCount (parse-int (:writeCount e)) failedCount (parse-int (:failedCount e))]\r\n")
            .append(
                "    (if (> writeCount 0)\r\n      (let [ratio (double (/ failedCount writeCount))]\r\n")
            .append("        (if (> ratio 0.1) ; Report if the failure count exceeds 10%\r\n")
            .append(
                "          (prn (:host e) \"has an unacceptable failure rate of\" ratio))))))\r\n\r\n")
            .append(
                "(defn check-95th-latency\r\n  \"Check if the 95th percentile latency is within expectations\"\r\n")
            .append("  [e]\r\n  (let [latency (parse-double (:latency95 e))]\r\n")
            .append(
                "    (if (> latency 1.0) ; Report if the 95th percentile latency exceeds 1.0s\r\n")
            .append(
                "      (prn (:host e) \"has an unacceptable 95th percentile latency of\" latency))))\r\n\r\n")
            .append("(streams\r\n  (where\r\n    (service #\".*LatencyReport.*\")")
            .append(
                " ; Only process services containing LatencyReport\r\n    check-failure-rate\r\n")
            .append("    check-95th-latency))");
    return sb.toString();
  }

  private void spectate(final String clusterName, final String resourceName, final int numPartitions)
      throws Exception {
    final Random random = new Random();
    final ClusterId clusterId = ClusterId.from(clusterName);
    final ResourceId resourceId = ResourceId.from(resourceName);

    // Connect to Helix
    final HelixManager manager =
        HelixManagerFactory.getZKHelixManager(clusterName, null, InstanceType.SPECTATOR, ZK_ADDR);
    manager.connect();

    // Attach a monitoring client to this connection
    final MonitoringClient client =
        new RiemannMonitoringClient(clusterId, manager.getHelixDataAccessor());
    client.connect();

    // Start spectating
    final RoutingTableProvider routingTableProvider = new RoutingTableProvider();
    manager.addExternalViewChangeListener(routingTableProvider);

    // Send some metrics
    client.every(5, 0, TimeUnit.SECONDS, new Runnable() {
      @Override
      public void run() {
        Map<ParticipantId, Integer> writeCounts = Maps.newHashMap();
        Map<ParticipantId, Integer> failedCounts = Maps.newHashMap();
        Map<ParticipantId, Double> latency95Map = Maps.newHashMap();
        for (int i = 0; i < numPartitions; i++) {
          // Figure out who hosts what
          PartitionId partitionId = PartitionId.from(resourceId, i + "");
          List<InstanceConfig> instances =
              routingTableProvider.getInstances(resourceName, partitionId.stringify(), "MASTER");
          if (instances.size() < 1) {
            continue;
          }

          // Normally you would get these attributes by using a CallTracker
          ParticipantId participantId = instances.get(0).getParticipantId();
          int writeCount = random.nextInt(1000) + 10;
          if (!writeCounts.containsKey(participantId)) {
            writeCounts.put(participantId, writeCount);
          } else {
            writeCounts.put(participantId, writeCounts.get(participantId) + writeCount);
          }
          int failedCount = i != 0 ? 0 : writeCount / 2; // bad write count from p0 master
          if (!failedCounts.containsKey(participantId)) {
            failedCounts.put(participantId, failedCount);
          } else {
            failedCounts.put(participantId, failedCounts.get(participantId) + failedCount);
          }
          double latency = (i != 1) ? 0.001 : 5.000; // bad 95th latency from p1 master
          latency95Map.put(participantId, latency);
        }

        // Send everything grouped by participant
        for (ParticipantId participantId : writeCounts.keySet()) {
          Map<String, String> attributes = Maps.newHashMap();
          attributes.put("writeCount", writeCounts.get(participantId) + "");
          attributes.put("failedCount", failedCounts.get(participantId) + "");
          attributes.put("latency95", latency95Map.get(participantId) + "");

          // Send an event with a ttl long enough to span the send interval
          MonitoringEvent e =
              new MonitoringEvent().cluster(clusterId).resource(resourceId)
                  .participant(participantId).name("LatencyReport").attributes(attributes)
                  .eventState("update").ttl(10.0f);
          client.send(e, false);
        }
      }
    });
    Thread.sleep(60000);
    client.disconnect();
    manager.disconnect();
  }

}
