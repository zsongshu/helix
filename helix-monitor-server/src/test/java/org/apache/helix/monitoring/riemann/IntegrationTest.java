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

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.controller.alert.AlertAction;
import org.apache.helix.controller.alert.AlertName;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.DefaultAlertMsgHandlerFactory;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.AlertConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MonitoringConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.monitoring.MonitoringEvent;
import org.apache.helix.monitoring.MonitoringTestHelper;
import org.apache.helix.monitoring.riemann.RiemannAgent;
import org.apache.helix.monitoring.riemann.RiemannConfigs;
import org.apache.helix.monitoring.riemann.RiemannMonitoringServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class IntegrationTest extends ZkUnitTestBase {
  @Test
  public void testBasic() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up monitoring cluster
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "MonitoringService", // resource name prefix
        1, // resources
        8, // partitions per resource
        0, // number of nodes
        1, // replicas
        "OnlineOffline", // pick a built-in state model
        RebalanceMode.FULL_AUTO, // let Helix handle rebalancing
        true); // do rebalance

    // Enable auto-join
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    configAccessor.set(scope, ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "" + true);

    // Start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // Start helix proxy
    final BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    final HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // Start monitoring server
    int riemannPort = MonitoringTestHelper.availableTcpPort();
    MonitoringConfig riemannConfig = new MonitoringConfig(RiemannConfigs.DEFAULT_RIEMANN_CONFIG);
    riemannConfig.setConfig(MonitoringTestHelper.getRiemannConfigString(riemannPort));

    MonitoringConfig latencyCheckConfig = new MonitoringConfig("check_latency_config.clj");
    latencyCheckConfig.setConfig(MonitoringTestHelper.getLatencyCheckConfigString(ZK_ADDR));

    // Set monitoring config on zk
    accessor.setProperty(keyBuilder.monitoringConfig(riemannConfig.getId()),
        riemannConfig);
    accessor.setProperty(keyBuilder.monitoringConfig(latencyCheckConfig.getId()),
        latencyCheckConfig);

    RiemannConfigs.Builder riemannConfigBuilder =
        new RiemannConfigs.Builder().addConfigs(Lists.newArrayList(riemannConfig,
            latencyCheckConfig));
    RiemannMonitoringServer server = new RiemannMonitoringServer(riemannConfigBuilder.build());
    server.start();

    // Start Riemann agent
    RiemannAgent agent = new RiemannAgent(ZK_ADDR, clusterName, riemannPort);
    agent.start();

    // Check live-instance
    List<String> liveInstances = accessor.getChildNames(keyBuilder.liveInstances());
    Assert.assertNotNull(liveInstances);
    Assert.assertEquals(liveInstances.size(), 1);

    boolean result;

    // Setup mock storage cluster to be monitored
    String storageClusterName = clusterName + "_storage";
    TestHelper.setupCluster(storageClusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        2, // number of nodes
        1, // replicas
        "MasterSlave", // pick a built-in state model
        RebalanceMode.FULL_AUTO, // let Helix handle rebalancing
        true); // do rebalance

    // Add alert config
    AlertConfig alertConfig = new AlertConfig("default");
    AlertName alertName =
        new AlertName.Builder().cluster(ClusterId.from(storageClusterName)).metric("latency95")
            .largerThan("1000").build();
    AlertAction alertAction =
        new AlertAction.Builder().cmd("enableInstance").args("{cluster}", "{node}", "false")
            .build();
    alertConfig.putConfig(alertName, alertAction);
    final HelixDataAccessor storageAccessor =
        new ZKHelixDataAccessor(storageClusterName, baseAccessor);
    final PropertyKey.Builder storageKeyBuilder = storageAccessor.keyBuilder();
    storageAccessor.setProperty(storageKeyBuilder.alertConfig("default"), alertConfig);

    // Start another controller for mock storage cluster
    ClusterControllerManager storageController =
        new ClusterControllerManager(ZK_ADDR, storageClusterName, "controller");
    MessageHandlerFactory fty = new DefaultAlertMsgHandlerFactory();
    storageController.getMessagingService()
        .registerMessageHandlerFactory(fty.getMessageType(), fty);
    storageController.syncStart();

    // Check localhost_12918 is enabled
    InstanceConfig instanceConfig =
        storageAccessor.getProperty(storageKeyBuilder.instanceConfig("localhost_12918"));
    Assert.assertTrue(instanceConfig.getInstanceEnabled());

    // Connect monitoring client
    final RiemannClientWrapper rclient =
        new RiemannClientWrapper(Arrays.asList("localhost:" + riemannPort));
    rclient.connect();

    MonitoringEvent event =
        new MonitoringEvent().participant(ParticipantId.from("localhost_12918"))
            .name("LatencyReport").attribute("latency95", "" + 2)
            .attribute("cluster", storageClusterName);
    rclient.send(event);

    // Check localhost_12918 is disabled
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        InstanceConfig instanceConfig =
            storageAccessor.getProperty(storageKeyBuilder.instanceConfig("localhost_12918"));
        return instanceConfig.getInstanceEnabled() == false;
      }
    }, 10 * 1000);
    Assert.assertTrue(result, "localhost_12918 should be disabled");

    // Cleanup
    rclient.disconnect();
    storageController.syncStop();
    controller.syncStop();

    agent.shutdown();
    server.stop();
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
