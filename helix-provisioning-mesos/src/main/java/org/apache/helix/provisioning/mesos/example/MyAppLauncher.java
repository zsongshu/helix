package org.apache.helix.provisioning.mesos.example;

import java.util.Map;

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixController;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ProvisionerConfigHolder;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.provisioning.mesos.MesosProvisionerConfig;
import org.apache.helix.tools.ClusterSetup;

import com.google.common.collect.ImmutableMap;

public class MyAppLauncher {
  public static void main(String[] args) {
    ClusterId clusterId = ClusterId.from("testApp");
    ResourceId resourceId = ResourceId.from("MyService");
    ClusterSetup setupTool = new ClusterSetup("localhost:2199");
    setupTool.addCluster(clusterId.toString(), true);
    Map<String, String> clusterConfigMap = ImmutableMap.of("allowParticipantAutoJoin", "true");
    setupTool.getClusterManagementTool().setConfig(
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterId.toString())
            .build(), clusterConfigMap);
    setupTool.addResourceToCluster(clusterId.toString(), resourceId.toString(), 1,
        "StatelessService", RebalanceMode.FULL_AUTO.toString());
    setupTool.getClusterManagementTool().rebalance(clusterId.toString(), resourceId.toString(), 1);
    HelixConnection connection = new ZkHelixConnection("localhost:2199");
    connection.connect();
    HelixDataAccessor accessor = connection.createDataAccessor(clusterId);
    MesosProvisionerConfig provisionerConfig = new MesosProvisionerConfig(resourceId);
    provisionerConfig.setAppClassName(TestMesosService.class.getName());
    provisionerConfig.setMasterAddress("127.0.0.1:5050");
    provisionerConfig.setNumContainers(2);
    ResourceConfiguration resourceConfiguration = new ResourceConfiguration(resourceId);
    ProvisionerConfigHolder holder = ProvisionerConfigHolder.from(provisionerConfig);
    resourceConfiguration.addNamespacedConfig(holder.toNamespacedConfig());
    accessor.setProperty(accessor.keyBuilder().resourceConfig(resourceId.toString()),
        resourceConfiguration);
    HelixController controller =
        connection.createController(clusterId, ControllerId.from("controller1"));
    controller.start();
  }
}
