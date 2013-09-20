package org.apache.helix.metamanager;

public interface ClusterInstanceInjector {
	public void addInstance(String connection);
	public void removeInstance(String connection);
}
