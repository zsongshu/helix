package org.apache.helix.metamanager.impl.local;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.metamanager.container.ContainerProcess;

import com.google.common.base.Preconditions;

/**
 * Singleton tracking metadata for VM-local containers spawned via
 * {@link LocalContainerProvider}.
 * 
 */
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
            for (LocalProcess local : processes.values()) {
                local.process.stop();
            }
            processes.clear();
        }
    }

    public static void killProcess(String id) throws InterruptedException {
        synchronized (processes) {
            Preconditions.checkArgument(processes.containsKey(id), "Process '%s' does not exist", id);
            ContainerProcess process = processes.get(id).process;
            process.stop();
            processes.remove(id);
        }
    }

    static class LocalProcess {
        final String           id;
        final String           owner;
        final ContainerProcess process;

        public LocalProcess(String id, String owner, ContainerProcess process) {
            this.id = id;
            this.owner = owner;
            this.process = process;
        }
    }

}
