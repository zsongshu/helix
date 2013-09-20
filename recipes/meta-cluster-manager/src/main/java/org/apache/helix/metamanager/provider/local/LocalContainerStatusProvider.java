package org.apache.helix.metamanager.provider.local;

import java.util.Map;

import org.apache.helix.metamanager.ClusterContainerStatusProvider;
import org.apache.helix.metamanager.provider.local.LocalContainerSingleton.LocalProcess;

public class LocalContainerStatusProvider implements ClusterContainerStatusProvider {

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
			return processes.get(id).process != null;
		}
	}

	@Override
	public boolean isFailed(String id) {
		Map<String, LocalProcess> processes = LocalContainerSingleton.getProcesses();
		
		synchronized (processes) {
			return processes.get(id).process == null;
		}
	}

}
