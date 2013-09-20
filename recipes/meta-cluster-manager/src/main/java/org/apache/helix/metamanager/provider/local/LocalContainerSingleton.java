package org.apache.helix.metamanager.provider.local;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.metamanager.managed.ContainerProcess;

public class LocalContainerSingleton {
	final static Map<String, LocalProcess> processes = new HashMap<String, LocalProcess>();

	private LocalContainerSingleton() {
		// left blank
	}
	
	public static Map<String, LocalProcess> getProcesses() {
		return processes;
	}
	
	public static void reset() {
		synchronized (processes) {
			for(LocalProcess local : processes.values()) {
				local.process.stop();
			}
			processes.clear();
		}
	}
	
	static class LocalProcess {
		final String id;
		final String owner;
		final ContainerProcess process;
		
		public LocalProcess(String id, String owner, ContainerProcess process) {
			this.id = id;
			this.owner = owner;
			this.process = process;
		}
	}

}
