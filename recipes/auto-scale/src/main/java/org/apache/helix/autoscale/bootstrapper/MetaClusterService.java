package org.apache.helix.autoscale.bootstrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.HelixAdmin;
import org.apache.helix.autoscale.Service;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

/**
 * Meta cluster bootstrapping. Create Helix data structures in zookeeper for
 * the meta cluster.
 * 
 */
public class MetaClusterService implements Service {

    static final Logger log = Logger.getLogger(MetaClusterService.class);

    String              name;
    String              address;
    String              managedCluster;
    String              managedAddress;

    @Override
    public void configure(Properties properties) throws Exception {
        name = properties.getProperty("name", "metacluster");
        address = properties.getProperty("address", "localhost:2199");
        managedCluster = properties.getProperty("managedcluster", "cluster");
        managedAddress = properties.getProperty("managedaddress", "localhost:2199");
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("setting up '%s/%s'", address, name));
        HelixAdmin admin = new ZKHelixAdmin(address);
        admin.addCluster(name, false);
        admin.addStateModelDef(name, "OnlineOffline", new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));

        HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER, name).build();
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("cluster", managedCluster);
        properties.put("address", managedAddress);
        admin.setConfig(scope, properties);

        admin.close();
        log.info("setup complete");
    }

    @Override
    public void stop() throws Exception {
        // left blank
    }

}
