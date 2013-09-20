package org.apache.helix.autoscale.bootstrapper;

import java.util.Properties;

import org.apache.helix.HelixAdmin;
import org.apache.helix.autoscale.Service;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

/**
 * Cluster bootstrapping. Create Helix data structures in zookeeper for the
 * managed cluster.
 * 
 */
public class ClusterService implements Service {

    static final Logger log = Logger.getLogger(ClusterService.class);

    String              name;
    String              address;

    @Override
    public void configure(Properties properties) throws Exception {
        name = properties.getProperty("name", "cluster");
        address = properties.getProperty("address", "localhost:2199");
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("setting up '%s/%s'", address, name));
        HelixAdmin admin = new ZKHelixAdmin(address);
        admin.addCluster(name, false);
        admin.addStateModelDef(name, "OnlineOffline", new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));
        admin.addStateModelDef(name, "MasterSlave", new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave()));
        admin.close();
        log.info("setup complete");
    }

    @Override
    public void stop() throws Exception {
        // left blank
    }

}
