package org.apache.helix.autoscale.impl.local;

import java.util.Map;
import java.util.Properties;

import org.apache.helix.autoscale.StatusProviderService;
import org.apache.helix.autoscale.impl.local.LocalContainerSingleton.LocalProcess;

/**
 * StatusProvider for VM-local containers spawned via
 * {@link LocalContainerProvider}. Runnable and configurable service.
 * 
 */
public class LocalStatusProvider implements StatusProviderService {

    @Override
    public boolean exists(String id) {
        Map<String, LocalProcess> processes = LocalContainerSingleton.getProcesses();

        synchronized (processes) {
            return processes.containsKey(id);
        }
    }

    @Override
    public boolean isHealthy(String id) {
        Map<String, LocalProcess> processes = LocalContainerSingleton.getProcesses();

        synchronized (processes) {
            LocalProcess local = processes.get(id);

            if (local == null)
                return false;

            return local.process.isActive();
        }
    }

    @Override
    public void configure(Properties properties) throws Exception {
        // left blank
    }

    @Override
    public void start() throws Exception {
        // left blank
    }

    @Override
    public void stop() throws Exception {
        // left blank
    }
}
