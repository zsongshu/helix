package org.apache.helix.metamanager;

import java.util.HashMap;
import java.util.Map;


public class StaticStatusProvider implements ClusterStatusProvider {

	final Map<String, Integer> targetCounts = new HashMap<String, Integer>();
	
	public StaticStatusProvider() {
	    // left blank
	}
	
	public StaticStatusProvider(Map<String, Integer> targetCounts) {
		this.targetCounts.putAll(targetCounts);
	}
	
	@Override
	public int getTargetContainerCount(String containerType) {
		return targetCounts.get(containerType);
	}

	public void setTargetContainerCount(String containerType, int targetCount) {
		targetCounts.put(containerType, targetCount);
	}

}
