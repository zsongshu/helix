package org.apache.helix.metamanager.managed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.helix.metamanager.ClusterContainerProvider;
import org.apache.log4j.Logger;

public class LocalProcessProvider implements ClusterContainerProvider {

	static final Logger log = Logger.getLogger(LocalProcessProvider.class);
	
	static final String REQUIRED_TYPE = "container";
	
	Map<String, ManagedProcess> processes = new HashMap<String, ManagedProcess>();
	Map<String, String> id2connection = new HashMap<String, String>();

	int connectionCounter = 0;
	
	final String zkAddress;
	final String clusterName;
	final int basePort;
	
	public LocalProcessProvider(String zkAddress, String clusterName, int basePort) {
		this.zkAddress = zkAddress;
		this.clusterName = clusterName;
		this.basePort = basePort;
	}

	@Override
	public synchronized String create(String id, String type) throws Exception {
		if(processes.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' already exists", id));
		
		if(!type.equals(REQUIRED_TYPE))
			throw new IllegalArgumentException(String.format("Type '%s' not supported", type));
		
		String connection = "localhost_" + (basePort + connectionCounter);
		connectionCounter++;
		
		log.info(String.format("Running container '%s' (zkAddress='%s', clusterName='%s', connection='%s')", id, zkAddress, clusterName, connection));
		
		ManagedProcess p = new ManagedProcess(clusterName, zkAddress, connection);
		
		processes.put(id, p);
		id2connection.put(id, connection);
		
		return connection;
	}
	
	public synchronized void start(String id) throws Exception {
		if(!processes.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' does not exists", id));
		
		log.info(String.format("Starting container '%s'", id));
		
		ManagedProcess p = processes.get(id);
		
		p.start();
	}
	
	public synchronized void stop(String id) throws Exception {
		if(!processes.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' does not exists", id));
		
		log.info(String.format("Stopping container '%s'", id));
		
		ManagedProcess p = processes.get(id);
		
		p.stop();
	}

	@Override
	public synchronized String destroy(String id) throws Exception {
		if(!processes.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' does not exists", id));
		
		log.info(String.format("Destroying container '%s'", id));
		
		String connection = id2connection.get(id);

		processes.remove(id);
		id2connection.remove(id);
		
		return connection;
	}
	
	public synchronized void destroyAll() {
		log.info("Destroying all processes");
		for(String id : new HashSet<String>(processes.keySet())) {
			try {
				destroy(id);
			} catch (Exception ignore) {
				// ignore
			}
		}
	}

}
