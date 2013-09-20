package org.apache.helix.metamanager;

public class TestStatusProvider implements ClusterStatusProvider {

	int targetContainerCount;
	
	public TestStatusProvider(int targetContainerCount) {
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
