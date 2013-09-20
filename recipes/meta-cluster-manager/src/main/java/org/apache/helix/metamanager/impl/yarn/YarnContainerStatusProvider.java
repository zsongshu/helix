package org.apache.helix.metamanager.impl.yarn;

import org.apache.helix.metamanager.ContainerStatusProvider;
import org.apache.helix.metamanager.impl.yarn.ContainerMetadata.ContainerState;
import org.apache.helix.metamanager.impl.yarn.MetadataService.MetadataServiceException;

public class YarnContainerStatusProvider implements ContainerStatusProvider {

	final String metadataAddress;
	
	ZookeeperMetadataService metaService;
	
	public YarnContainerStatusProvider(String metadataAddress) {
		this.metadataAddress = metadataAddress;
		this.metaService = new ZookeeperMetadataService(metadataAddress);
	}

	@Override
	public boolean exists(String id) {
		return metaService.exists(id);
	}

	@Override
	public boolean isActive(String id) {
		try {
			return metaService.read(id).state == ContainerState.ACTIVE;
		} catch (MetadataServiceException e) {
			return false;
		}
	}

	@Override
	public boolean isFailed(String id) {
		try {
			return metaService.read(id).state == ContainerState.FAILED;
		} catch (Exception e) {
			return false;
		}
	}

	public void startService() {
		metaService = new ZookeeperMetadataService(metadataAddress);
		metaService.startService();
	}
	
	public void stopService() {
		if(metaService != null) {
			metaService.stopService();
			metaService = null;
		}
	}
}
