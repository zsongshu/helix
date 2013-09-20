package org.apache.helix.metamanager.provider.shell;

import java.util.Map;

import org.apache.helix.metamanager.ClusterContainerStatusProvider;
import org.apache.helix.metamanager.provider.shell.ShellContainerSingleton.ShellProcess;

public class ShellContainerStatusProvider implements ClusterContainerStatusProvider {

	@Override
	public boolean exists(String id) {
		Map<String, ShellProcess> processes = ShellContainerSingleton.getProcesses();

		synchronized (processes) {
			return processes.containsKey(id);
		}
	}

	@Override
	public boolean isActive(String id) {
		Map<String, ShellProcess> processes = ShellContainerSingleton.getProcesses();

		synchronized (processes) {
			ShellProcess shell = processes.get(id);
			
			try {
				shell.process.exitValue();
				return false;
			} catch (IllegalThreadStateException e) {
				// still running
				return true;
			}
		}
	}

	@Override
	public boolean isFailed(String id) {
		Map<String, ShellProcess> processes = ShellContainerSingleton.getProcesses();

		synchronized (processes) {
			ShellProcess shell = processes.get(id);
			
			try {
				return (shell.process.exitValue() != 0);
			} catch (IllegalThreadStateException e) {
				// still running
				return false;
			}
		}
	}

}
