package org.apache.helix.metamanager.provider.shell;

import java.util.HashMap;
import java.util.Map;

public class ShellContainerSingleton {
	static final Map<String, ShellProcess> processes = new HashMap<String, ShellProcess>();

	private ShellContainerSingleton() {
		// left blank
	}
	
	public static Map<String, ShellProcess> getProcesses() {
		return processes;
	}
	
	public static void reset() {
		synchronized (processes) {
			for(ShellProcess local : processes.values()) {
				local.process.destroy();
				try { local.process.waitFor(); } catch(Exception ignore) {}
			}
			processes.clear();
		}
	}
	
	static class ShellProcess {
		final String id;
		final String owner;
		final Process process;

		public ShellProcess(String id, String owner, Process process) {
			this.id = id;
			this.owner = owner;
			this.process = process;
		}		
	}
}
