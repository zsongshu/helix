package org.apache.helix.metamanager.provider.yarn;

import java.util.concurrent.TimeoutException;

import org.apache.helix.metamanager.ClusterContainerProvider;
import org.apache.helix.metamanager.provider.yarn.ContainerMetadata.ContainerState;
import org.apache.helix.metamanager.provider.yarn.MetadataService.MetadataServiceException;
import org.apache.log4j.Logger;

public class YarnContainerProvider implements ClusterContainerProvider {
	
	static final Logger log = Logger.getLogger(YarnContainerProvider.class);

	static final long POLL_INTERVAL = 1000;
	
	static final String REQUIRED_TYPE = "container";
	
	static final long CONTAINER_TIMEOUT = 10000;
	
	/*
	 * CONTAINERS
	 *   A (A, READY)
	 *   B (B, RUNNING)
	 */
	
	final ApplicationConfig appConfig;
	final String command;
	
	final Object notifier = new Object();
	
	ZookeeperMetadataService metaService;
	
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
		waitForState(id, ContainerState.ACTIVE);
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
		
		waitForState(id, ContainerState.FINALIZE);
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
		log.debug("Starting yarn container provider service");
		metaService = new ZookeeperMetadataService(appConfig.metadataAddress);
		metaService.startService();
	}
	
	public void stopService() {
		log.debug("Stopping yarn container provider service");
		if(metaService != null) {
			metaService.stopService();
			metaService = null;
		}
	}
	
	void waitForState(String id, ContainerState state) throws MetadataServiceException, InterruptedException, TimeoutException {
		long limit = System.currentTimeMillis() + CONTAINER_TIMEOUT;
		ContainerMetadata meta = metaService.read(id);
		while(meta.state != state) {
			if(System.currentTimeMillis() >= limit) {
				throw new TimeoutException(String.format("Container '%s' failed to reach state '%s' (currently is '%s')", id, state, meta.state));
			}
			Thread.sleep(POLL_INTERVAL);
			meta = metaService.read(id);
		}
	}
	
}
