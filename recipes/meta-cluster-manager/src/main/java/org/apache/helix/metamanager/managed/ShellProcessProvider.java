package org.apache.helix.metamanager.managed;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.helix.metamanager.ClusterContainerProvider;
import org.apache.log4j.Logger;

public class ShellProcessProvider implements ClusterContainerProvider {

	static final Logger log = Logger.getLogger(ShellProcessProvider.class);
	
	static final String REQUIRED_TYPE = "container";
	static final String RUN_COMMAND = "/bin/sh";
	static final String KILL_COMMAND = "kill -s SIGINT %d";
	
	Map<String, ProcessBuilder> builders = new HashMap<String, ProcessBuilder>();
	Map<String, Process> processes = new HashMap<String, Process>();
	Map<String, String> id2connection = new HashMap<String, String>();

	int connectionCounter = 0;
	
	final String zkAddress;
	final String clusterName;
	final int basePort;
	final String command;
	
	public ShellProcessProvider(String zkAddress, String clusterName, int basePort, String command) {
		this.zkAddress = zkAddress;
		this.clusterName = clusterName;
		this.basePort = basePort;
		this.command = command;
	}

	@Override
	public synchronized String create(String id, String type) throws Exception {
		if(builders.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' already exists", id));
		
		if(!type.equals(REQUIRED_TYPE))
			throw new IllegalArgumentException(String.format("Type '%s' not supported", type));
		
		String connection = "localhost_" + (basePort + connectionCounter);
		connectionCounter++;
		
		log.info(String.format("Running container '%s' (zkAddress='%s', clusterName='%s', connection='%s', command='%s')", id, zkAddress, clusterName, connection, command));
		
		ProcessBuilder builder = new ProcessBuilder(RUN_COMMAND, command, zkAddress, clusterName, connection);
		
		builders.put(id, builder);
		id2connection.put(id, connection);
		
		return connection;
	}
	
	public synchronized void start(String id) throws Exception {
		if(!builders.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' does not exists", id));
		
		if(processes.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' already running", id));
		
		log.info(String.format("Starting container '%s'", id));
		
		Process p = builders.get(id).start();
		
		processes.put(id, p);
	}
	
	public synchronized void stop(String id) throws Exception {
		if(!builders.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' does not exists", id));
		
		if(!processes.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' not running", id));
		
		log.info(String.format("Stopping container '%s'", id));
		
		Process p = processes.get(id);
		
		int pid = getUnixPID(p);
		Runtime.getRuntime().exec(String.format(KILL_COMMAND, pid));
		
		int retVal = p.waitFor();
		if(retVal != 130) {
			log.warn(String.format("Process %d returned %d (should be 130, SIGINT)", pid, retVal));
		}
		
		processes.remove(id);
		
	}

	@Override
	public synchronized String destroy(String id) throws Exception {
		if(!builders.containsKey(id))
			throw new IllegalArgumentException(String.format("Process '%s' does not exists", id));
		
		log.info(String.format("Destroying container '%s'", id));
		
		if(processes.containsKey(id)) {
			log.warn(String.format("Forcibly terminating running container '%s'", id));
			processes.get(id).destroy();
			processes.remove(id);
		}
		
		String connection = id2connection.get(id);

		builders.remove(id);
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
	
	// TODO get PID independently of platform
    static int getUnixPID(Process process) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException {
        if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
            Class<?> proc = process.getClass();
            Field field = proc.getDeclaredField("pid");
            Object value = getFieldValue(field, process);
            return ((Integer) value).intValue();
        } else {
            throw new IllegalArgumentException("Not a UNIXProcess");
        }
    }
    
    static Object getFieldValue(Field field, Object object) throws IllegalArgumentException, IllegalAccessException {
    	Object value;
    	boolean accessible = field.isAccessible();
    	field.setAccessible(true);
    	value = field.get(object);
    	field.setAccessible(accessible);
    	return value;
    }

}
