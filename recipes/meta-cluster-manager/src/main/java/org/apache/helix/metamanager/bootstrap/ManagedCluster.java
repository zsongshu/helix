package org.apache.helix.metamanager.bootstrap;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

public class ManagedCluster {

    static final Logger log = Logger.getLogger(ManagedCluster.class);

    public static final String DEFAULT_CLUSTER = "managed";
    
    Properties          properties;

    HelixAdmin          admin;
    HelixManager        controllerMananger;

    public void start() {
        String cluster = properties.getProperty("cluster", DEFAULT_CLUSTER);
        String address = properties.getProperty("address");

        log.info(String.format("starting managed cluster service (cluster='%s', address='%s')", cluster, address));

        log.debug("setting up cluster admin");
        admin = new ZKHelixAdmin(address);
        admin.addCluster(cluster, false);
        admin.addStateModelDef(cluster, "OnlineOffline", new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));
        admin.addStateModelDef(cluster, "MasterSlave", new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave()));

        log.debug("setting up resources");
        String resources = properties.getProperty("resources");
        String[] resourceNames = StringUtils.split(resources, ",");

        for (String resourceName : resourceNames) {
            Properties properties = BootUtils.getNamespace(BootUtils.getNamespace(this.properties, "resource"), resourceName);

            log.debug(String.format("parsing resource '%s' (properties='%s')", resourceName, properties));

            String container = properties.getProperty("container");
            String model = properties.getProperty("model");
            int partitions = Integer.parseInt(properties.getProperty("partitions"));
            int replica = Integer.parseInt(properties.getProperty("replica"));

            log.debug(String.format("setting up resource '%s' (container='%s', model='%s', partitions=%d, replica=%d)", resourceName, container, model,
                    partitions, replica));

            admin.addResource(cluster, resourceName, partitions, model, RebalanceMode.FULL_AUTO.toString());
            IdealState idealState = admin.getResourceIdealState(cluster, resourceName);
            idealState.setInstanceGroupTag(container);
            idealState.setReplicas(String.valueOf(replica));
            admin.setResourceIdealState(cluster, resourceName, idealState);
        }

        log.debug("setting up controller");
        controllerMananger = HelixControllerMain.startHelixController(address, cluster, "managedController", HelixControllerMain.STANDALONE);
    }

    public void stop() {
        log.info("stopping managed cluster service");
        if (controllerMananger != null) {
            controllerMananger.disconnect();
            controllerMananger = null;
        }
        if (admin != null) {
            admin.close();
            admin = null;
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

}
