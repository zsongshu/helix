package org.apache.helix.metamanager.managed;

import org.apache.helix.HelixAdmin;
import org.apache.helix.metamanager.ClusterAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

public class LocalClusterManager implements ClusterAdmin {
	
	static final Logger log = Logger.getLogger(LocalClusterManager.class);

	final String clusterName;
	final String resourceName;
	final int replica;
	final HelixAdmin admin;
	
	public LocalClusterManager(String clusterName, String resourceName,
			int replica, HelixAdmin admin) {
		this.clusterName = clusterName;
		this.resourceName = resourceName;
		this.replica = replica;
		this.admin = admin;
	}

	@Override
	public synchronized void addInstance(String connection) {
		log.debug(String.format("injecting instance %s in cluster %s", connection, clusterName));
		admin.addInstance(clusterName, new InstanceConfig(connection));
	}

	@Override
	public synchronized void removeInstance(String connection) {
		log.debug(String.format("removing instance %s from cluster %s", connection, clusterName));
		admin.dropInstance(clusterName, new InstanceConfig(connection));
	}

	@Override
	public void rebalance() {
		admin.rebalance(clusterName, resourceName, replica);
	}
	
}
