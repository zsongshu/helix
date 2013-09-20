package org.apache.helix.metamanager;

public interface ClusterContainerStatusProvider {
	public boolean exists(String id);
	public boolean isActive(String id);
	public boolean isFailed(String id);
}
