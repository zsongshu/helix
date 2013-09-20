package org.apache.helix.metamanager;

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

/**
 * Implementation of ClusterAdmin based on Helix.
 * 
 */
public class HelixClusterAdmin implements ClusterAdmin {

    static final Logger log = Logger.getLogger(HelixClusterAdmin.class);

    final String        cluster;
    final HelixAdmin    admin;

    public HelixClusterAdmin(String clusterName, HelixAdmin admin) {
        this.cluster = clusterName;
        this.admin = admin;
    }

    @Override
    public synchronized void addInstance(String instanceId, String instanceTag) {
        log.debug(String.format("injecting instance %s (tag=%s) in cluster %s", instanceId, instanceTag, cluster));
        admin.addInstance(cluster, new InstanceConfig(instanceId));
        admin.addInstanceTag(cluster, instanceId, instanceTag);
    }

    @Override
    public synchronized void removeInstance(String connection) {
        log.debug(String.format("removing instance %s from cluster %s", connection, cluster));
        admin.dropInstance(cluster, new InstanceConfig(connection));
    }

    @Override
    public void rebalance() {
        for (String resourceName : admin.getResourcesInCluster(cluster)) {
            int replica = Integer.parseInt(admin.getResourceIdealState(cluster, resourceName).getReplicas());
            admin.rebalance(cluster, resourceName, replica);
        }
    }
}
