package org.apache.helix.metamanager.impl.local;

import java.util.Map;

import org.apache.helix.metamanager.ContainerStatusProvider;
import org.apache.helix.metamanager.impl.local.LocalContainerSingleton.LocalProcess;

public class LocalContainerStatusProvider implements ContainerStatusProvider {

	@Override
	public boolean exists(String id) {
		Map<String, LocalProcess> processes = LocalContainerSingleton.getProcesses();
		
		synchronized (processes) {
			return processes.containsKey(id);
		}
	}

	@Override
	public boolean isActive(String id) {
		Map<String, LocalProcess> processes = LocalContainerSingleton.getProcesses();
		
		synchronized (processes) {
			return processes.get(id).process.isActive();
		}
	}

	@Override
	public boolean isFailed(String id) {
		Map<String, LocalProcess> processes = LocalContainerSingleton.getProcesses();
		
		synchronized (processes) {
			return processes.get(id).process.isFailed();
		}
	}

}
