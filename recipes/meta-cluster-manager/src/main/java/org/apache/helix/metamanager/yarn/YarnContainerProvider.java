package org.apache.helix.metamanager.yarn;

import org.apache.helix.metamanager.ClusterContainerProvider;
import org.apache.helix.metamanager.yarn.ContainerMetadata.ContainerState;
import org.apache.log4j.Logger;

public class YarnContainerProvider implements ClusterContainerProvider {
	
	static final Logger log = Logger.getLogger(YarnContainerProvider.class);

	static final String REQUIRED_TYPE = "container";
	
	static final long LOCK_TIMEOUT = 1000;
	static final long CONTAINER_TIMEOUT = 10000;
	
	/*
	 * CONTAINERS
	 *   A (A, READY)
	 *   B (B, RUNNING)
	 */
	
	final ApplicationConfig appConfig;
	final String command;
	
	final Object notifier = new Object();
	
	MetadataService metaService;
	
	public YarnContainerProvider(ApplicationConfig appConfig, String command) {
		this.appConfig = appConfig;
		this.command = command;
	}

	@Override
	public void create(final String id, final String type) throws Exception {
		if(!REQUIRED_TYPE.equals(type)) {
			throw new IllegalArgumentException(String.format("Type '%s' not supported", type));
		}
		
		metaService.create(new ContainerMetadata(id, command, appConfig.providerName));
		metaService.waitForState(id, ContainerState.ACTIVE, CONTAINER_TIMEOUT);
	}

	@Override
	public void destroy(final String id) throws Exception {
		ContainerMetadata meta = metaService.read(id);

		if(meta.state == ContainerState.ACTIVE) {
			log.info(String.format("Destroying active container, going to teardown"));
			metaService.update(new ContainerMetadata(meta, ContainerState.TEARDOWN));
			
		} else if(meta.state == ContainerState.FAILED) {
			log.info(String.format("Destroying failed container, going to halted"));
			metaService.update(new ContainerMetadata(meta, ContainerState.HALTED));
			
		} else if(meta.state == ContainerState.FINALIZE) {
			log.info(String.format("Destroying finalized container, skipping"));
			
		} else {
			throw new IllegalStateException(String.format("Container '%s' must be active, failed or finalized", id));
		}
		
		metaService.waitForState(id, ContainerState.FINALIZE, CONTAINER_TIMEOUT);
		metaService.delete(id);
	}

	@Override
	public void destroyAll() {
		try {
			for(ContainerMetadata meta : metaService.readAll()) {
				try { destroy(meta.id); } catch (Exception ignore) {}
			}
		} catch (Exception ignore) {
			// ignore
		}
	}

	public void startService() {
		metaService = new MetadataService(appConfig);
		metaService.start();
	}
	
	public void stopService() {
		if(metaService != null) {
			metaService.stop();
			metaService = null;
		}
	}
	
}
