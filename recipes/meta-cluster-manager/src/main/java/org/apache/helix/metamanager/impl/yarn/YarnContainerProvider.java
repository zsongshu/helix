package org.apache.helix.metamanager.impl.yarn;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.helix.metamanager.ContainerProvider;
import org.apache.helix.metamanager.ContainerProviderService;
import org.apache.helix.metamanager.impl.yarn.YarnContainerData.ContainerState;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * {@link ContainerProvider} spawning YARN-based containers. Reads and writes
 * meta data using {@link YarnDataProvider}. Works in a distributed setting, but
 * typically requires access to zookeeper.
 * 
 */
class YarnContainerProvider implements ContainerProviderService {
	
	static final Logger log = Logger.getLogger(YarnContainerProvider.class);

	static final long POLL_INTERVAL = 1000;
	static final long CONTAINER_TIMEOUT = 60000;
	
	/*
	 * CONTAINERS
	 *   A (A, READY)
	 *   B (B, RUNNING)
	 */
	
	final Object notifier = new Object();
    final Map<String, Properties> types = new HashMap<String, Properties>();
	
	ZookeeperYarnDataProvider yarnDataService;
	YarnContainerProviderProcess yarnApp;
	YarnContainerProviderProperties properties;
	
    @Override
    public void configure(Properties properties) throws Exception {
        YarnContainerProviderProperties yarnProps = new YarnContainerProviderProperties();
        yarnProps.putAll(properties);
        configure(yarnProps);
    }
    
    private void configure(YarnContainerProviderProperties properties) {
        this.properties = properties;
        
        for(String containerType : properties.getContainers()) {
            registerType(containerType, properties.getContainer(containerType));
        }
    }
    
    @Override
    public void start() throws Exception {
        Preconditions.checkNotNull(properties);
        Preconditions.checkState(properties.isValid(), "provider properties not valid: %s", properties);
        
        log.debug("Starting yarn container provider service");
        yarnDataService = new ZookeeperYarnDataProvider();
        yarnDataService.configure(properties);
        yarnDataService.start();
    }
    
    @Override
    public void stop() throws Exception {
        log.debug("Stopping yarn container provider service");
        destroyAll();
        
        if(yarnDataService != null) {
            yarnDataService.stop();
            yarnDataService = null;
        }
    }
    
	@Override
	public void create(final String id, final String type) throws Exception {
	    Preconditions.checkArgument(types.containsKey(type), "Container type '%s' is not configured", type);
	    
		YarnContainerProcessProperties containerProperties = YarnUtils.createContainerProcessProperties(types.get(type));

        log.info(String.format("Running container '%s' (properties='%s')", id, containerProperties));
        
		yarnDataService.create(new YarnContainerData(id, properties.getName(), containerProperties));
		waitForState(id, ContainerState.ACTIVE);
	}

	@Override
	public void destroy(final String id) throws Exception {
		YarnContainerData meta = yarnDataService.read(id);

		if(meta.state == ContainerState.ACTIVE) {
			log.info(String.format("Destroying active container, going to teardown"));
			yarnDataService.update(meta.setState(ContainerState.TEARDOWN));
			
		} else if(meta.state == ContainerState.FAILED) {
			log.info(String.format("Destroying failed container, going to teardown"));
			yarnDataService.update(meta.setState(ContainerState.TEARDOWN));
			
		} else if(meta.state == ContainerState.FINALIZE) {
			log.info(String.format("Destroying finalized container, skipping"));
			
		} else {
			throw new IllegalStateException(String.format("Container '%s' must be active, failed or finalized", id));
		}
		
		waitForState(id, ContainerState.FINALIZE);
		yarnDataService.delete(id);
	}

	@Override
	public void destroyAll() {
		try {
			for(YarnContainerData meta : yarnDataService.readAll()) {
			    if(meta.owner.equals(properties.getName())) {
			        try { destroy(meta.id); } catch (Exception ignore) {}
			    }
			}
		} catch (Exception ignore) {
			// ignore
		}
	}

	void waitForState(String id, ContainerState state) throws Exception, InterruptedException, TimeoutException {
		long limit = System.currentTimeMillis() + CONTAINER_TIMEOUT;
		YarnContainerData meta = yarnDataService.read(id);
		while(meta.state != state) {
			if(System.currentTimeMillis() >= limit) {
				throw new TimeoutException(String.format("Container '%s' failed to reach state '%s' (currently is '%s')", id, state, meta.state));
			}
			Thread.sleep(POLL_INTERVAL);
			meta = yarnDataService.read(id);
		}
	}
	
    void registerType(String name, Properties properties) {
        log.debug(String.format("Registering container type '%s' (properties='%s')", name, properties));
        types.put(name, properties);
    }

}
