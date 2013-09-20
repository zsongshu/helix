package org.apache.helix.metamanager.managed;

import org.apache.helix.metamanager.ClusterStatusProvider;

public class LocalStatusProvider implements ClusterStatusProvider {

	int targetContainerCount;
	
	public LocalStatusProvider(int targetContainerCount) {
		this.targetContainerCount = targetContainerCount;
	}

	@Override
	public int getTargetContainerCount(String type) {
		return targetContainerCount;
	}

	public void setTargetContainerCount(int targetContainerCount) {
		this.targetContainerCount = targetContainerCount;
	}

}
