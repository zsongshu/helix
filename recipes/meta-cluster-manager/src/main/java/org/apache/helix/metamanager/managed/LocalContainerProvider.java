package org.apache.helix.metamanager.managed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.helix.metamanager.ClusterContainerProvider;
import org.apache.log4j.Logger;

public class LocalContainerProvider implements ClusterContainerProvider {

	static final Logger log = Logger.getLogger(LocalContainerProvider.class);
	
	static final String REQUIRED_TYPE = "container";
	
	// global view of processes required
	static final Object staticLock = new Object();
	static final Map<String, LocalProcess> processes = new HashMap<String, LocalProcess>();
	
	int connectionCounter = 0;
	
	final String zkAddress;
	final String clusterName;
	final String providerName;
	
	public LocalContainerProvider(String zkAddress, String clusterName, String providerName) {
		this.zkAddress = zkAddress;
		this.clusterName = clusterName;
		this.providerName = providerName;
	}

	@Override
	public void create(String id, String type) throws Exception {
		synchronized (staticLock) {	
			if(processes.containsKey(id))
				throw new IllegalArgumentException(String.format("Process '%s' already exists", id));
			
			if(!type.equals(REQUIRED_TYPE))
				throw new IllegalArgumentException(String.format("Type '%s' not supported", type));
			
			log.info(String.format("Running container '%s' (zkAddress='%s', clusterName='%s')", id, zkAddress, clusterName));
			
			ManagedProcess process = new ManagedProcess(clusterName, zkAddress, id);
			process.start();
		
			processes.put(id, new LocalProcess(id, providerName, process));
			
		}
	}
	
	@Override
	public void destroy(String id) throws Exception {
		synchronized (staticLock) {	
			if(!processes.containsKey(id))
				throw new IllegalArgumentException(String.format("Process '%s' does not exists", id));
			
			log.info(String.format("Destroying container '%s'", id));
			
			LocalProcess local = processes.remove(id);
			
			local.process.stop();
		}
	}
	
	@Override
	public void destroyAll() {
		synchronized (staticLock) {	
			log.info("Destroying all processes");
			for(String id : new HashSet<String>(processes.keySet())) {
				try { destroy(id); } catch (Exception ignore) {}
			}
		}
	}
	
	static class LocalProcess {
		final String id;
		final String owner;
		final ManagedProcess process;
		
		public LocalProcess(String id, String owner, ManagedProcess process) {
			this.id = id;
			this.owner = owner;
			this.process = process;
		}
	}

}
