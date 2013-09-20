package org.apache.helix.metamanager.yarn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.helix.metamanager.yarn.ContainerMetadata.ContainerState;
import org.apache.log4j.Logger;

public class MetadataService {
	
	static final Logger log = Logger.getLogger(MetadataService.class);
	
	static final String CONTAINER_NAMESPACE = "containers";
	
//	static final String LOCK_PATH = "/" + CONTAINER_NAMESPACE + "/lock";
	static final long POLL_INTERVAL = 100;

	final ApplicationConfig appConfig;
	
	ZkClient client;
	String basePath;
	
	public MetadataService(ApplicationConfig appConfig) {
		this.appConfig = appConfig;
	}

	public void start() {
		basePath = "/" + CONTAINER_NAMESPACE;
		log.debug(String.format("starting metadata service for '%s/%s'", appConfig.providerAddress, appConfig.providerName));
		
		client = new ZkClient(appConfig.providerAddress);
		
		client.createPersistent(basePath, true);
	}
	
	public void stop() {
		log.debug(String.format("stopping metadata service for '%s/%s'", appConfig.providerAddress, appConfig.providerName));
		if(client != null) {
			client.close();
			client = null;
		}
	}
	
//	public void lock(long timeout) throws Exception {
//		long limit = System.currentTimeMillis() + timeout;
//		while (limit > System.currentTimeMillis()) {
//			try {
//				client.createEphemeral(LOCK_PATH);
//				return;
//			} catch (Exception ignore) {}
//			Thread.sleep(POLL_INTERVAL);
//		}
//		throw new IllegalStateException("Could not acquire lock");
//	}
//	
//	public void unlock() {
//		client.delete(LOCK_PATH);
//	}
	
	public void create(ContainerMetadata meta) throws IllegalMetadataStateException {
		try {
			client.createPersistent(makePath(meta.id), Utils.toJson(meta));
		} catch (ZkException e) {
			throw new IllegalMetadataStateException(e);
		}
	}
	
	public ContainerMetadata read(String id) throws IllegalMetadataStateException {
		try {
			return Utils.fromJson(client.<String>readData(makePath(id)));
		} catch (ZkException e) {
			throw new IllegalMetadataStateException(e);
		}
	}
	
	public Collection<ContainerMetadata> readAll() throws IllegalMetadataStateException {
		try {
			Collection<ContainerMetadata> metadata = new ArrayList<ContainerMetadata>();
			for(String id : client.getChildren(basePath)) {
				metadata.add(Utils.fromJson(client.<String>readData(makePath(id))));
			}
			return metadata;
		} catch (ZkException e) {
			throw new IllegalMetadataStateException(e);
		}
	}
	
	public void update(ContainerMetadata meta) throws IllegalMetadataStateException {
		try {
			client.writeData(makePath(meta.id), Utils.toJson(meta));
		} catch (ZkException e) {
			throw new IllegalMetadataStateException(e);
		}
	}
	
	public void delete(String id) throws IllegalMetadataStateException {
		try {
			client.delete(makePath(id));
		} catch (ZkException e) {
			throw new IllegalMetadataStateException(e);
		}
	}
	
	public void waitForState(String id, ContainerState state, long timeout) throws IllegalMetadataStateException, InterruptedException, TimeoutException {
		long limit = System.currentTimeMillis() + timeout;
		ContainerMetadata meta = read(id);
		while(meta.state != state) {
			if(System.currentTimeMillis() >= limit) {
				throw new TimeoutException(String.format("Container '%s' failed to reach state '%s' (currently is '%s')", id, state, meta.state));
			}
			Thread.sleep(POLL_INTERVAL);
			meta = read(id);
		}
	}
	
	String makePath(String containerId) {
		return basePath + "/" + containerId;
	}
	
	public static class IllegalMetadataStateException extends Exception {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2846997013918977056L;

		public IllegalMetadataStateException() {
			super();
		}

		public IllegalMetadataStateException(String message, Throwable cause) {
			super(message, cause);
		}

		public IllegalMetadataStateException(String message) {
			super(message);
		}

		public IllegalMetadataStateException(Throwable cause) {
			super(cause);
		}	
	}
}