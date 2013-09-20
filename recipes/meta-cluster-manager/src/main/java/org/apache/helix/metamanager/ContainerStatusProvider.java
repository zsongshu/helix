package org.apache.helix.metamanager;

public interface ContainerStatusProvider {
	public boolean exists(String id);
	public boolean isActive(String id);
	public boolean isFailed(String id);
}
