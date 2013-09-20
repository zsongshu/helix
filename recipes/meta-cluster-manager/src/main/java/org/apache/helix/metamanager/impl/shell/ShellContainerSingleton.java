package org.apache.helix.metamanager.impl.shell;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

/**
 * Singleton tracking metadata for shell-based containers spawned via
 * {@link ShellContainerProvider}.
 * 
 */
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
            for (ShellProcess shell : processes.values()) {
                shell.process.destroy();
				try { shell.process.waitFor(); } catch(Exception ignore) {}
            }
            processes.clear();
        }
    }

    public static void killProcess(String id) throws InterruptedException {
        synchronized (processes) {
            Preconditions.checkArgument(processes.containsKey(id), "Process '%s' does not exist", id);
            Process process = processes.get(id).process;
            process.destroy();
            process.waitFor();
            processes.remove(id);
        }
    }

    static class ShellProcess {
        final String  id;
        final String  owner;
        final Process process;
        final File    tmpDir;

        public ShellProcess(String id, String owner, Process process, File tmpDir) {
            this.id = id;
            this.owner = owner;
            this.process = process;
            this.tmpDir = tmpDir;
        }
    }
}
