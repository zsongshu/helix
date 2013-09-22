package org.apache.helix.autoscale;

import java.util.Properties;

public class MyAppTargetProvider implements TargetProviderService {
	private int _minCount = 0;
	private int _maxCount = 0;

	@Override
	public int getTargetContainerCount(String containerType) throws Exception {
		return _minCount; //TODO: Change this
	}

	@Override
	public void configure(Properties properties) throws Exception {
		String minContainersStr = (String) properties.get("minContainers");
		String maxContainersStr = (String) properties.get("maxContainers");

		if(minContainersStr != null) {
			_minCount = Integer.parseInt(minContainersStr);
		}
		
		if(maxContainersStr != null) {
			_maxCount = Integer.parseInt(maxContainersStr);
		}
	}

	@Override
	public void start() throws Exception {
		
	}

	@Override
	public void stop() throws Exception {
		
	}

}
