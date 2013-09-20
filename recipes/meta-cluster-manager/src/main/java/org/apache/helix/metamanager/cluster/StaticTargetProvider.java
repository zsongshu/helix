package org.apache.helix.metamanager.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.helix.metamanager.ClusterStatusProvider;


public class StaticTargetProvider implements ClusterStatusProvider {

	final Map<String, Integer> targetCounts = new HashMap<String, Integer>();
	
	public StaticTargetProvider() {
	    // left blank
	}
	
	public StaticTargetProvider(Properties properties) {
	    for(Entry<Object, Object> entry : properties.entrySet()) {
	        String key = (String)entry.getKey();
	        int value = Integer.valueOf((String)entry.getValue());
	        
	        targetCounts.put(key, value);
	    }
	}
	
	public StaticTargetProvider(Map<String, Integer> targetCounts) {
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
