package org.apache.helix.metamanager.bootstrapper;

import java.util.Properties;

import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.metamanager.Service;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.log4j.Logger;

/**
 * Bootstrapping Helix resource. Create resource in Helix and configure
 * properties.
 * 
 */
public class ResourceService implements Service {

    static final Logger log = Logger.getLogger(ResourceService.class);

    String              cluster;
    String              address;
    String              container;
    String              name;
    String              model;
    int                 partitions;
    int                 replica;

    @Override
    public void configure(Properties properties) throws Exception {
        cluster = properties.getProperty("cluster", "cluster");
        address = properties.getProperty("address", "localhost:2199");
        name = properties.getProperty("name", "resource");
        container = properties.getProperty("container", "container");
        model = properties.getProperty("model", "OnlineOffline");
        partitions = Integer.parseInt(properties.getProperty("partitions", "1"));
        replica = Integer.parseInt(properties.getProperty("replica", "1"));
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("setting up resource '%s' at '%s/%s'", name, address, cluster));
        HelixAdmin admin = new ZKHelixAdmin(address);

        log.info(String.format("setting up resource '%s' (container='%s', model='%s', partitions=%d, replica=%d)", name, container, model, partitions, replica));

        admin.addResource(cluster, name, partitions, model, RebalanceMode.FULL_AUTO.toString());
        IdealState idealState = admin.getResourceIdealState(cluster, name);
        idealState.setInstanceGroupTag(container);
        idealState.setReplicas(String.valueOf(replica));
        admin.setResourceIdealState(cluster, name, idealState);
        admin.close();
        log.info("setup complete");
    }

    @Override
    public void stop() throws Exception {
        // left blank
    }

}
