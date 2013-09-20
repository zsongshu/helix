package org.apache.helix.metamanager;

public interface ClusterStatusProvider {
	public int getTargetContainerCount(String containerType) throws Exception;
}
