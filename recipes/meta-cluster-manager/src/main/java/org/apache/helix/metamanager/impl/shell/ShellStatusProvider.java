package org.apache.helix.metamanager.impl.shell;

import java.util.Map;
import java.util.Properties;

import org.apache.helix.metamanager.StatusProviderService;
import org.apache.helix.metamanager.impl.shell.ShellContainerSingleton.ShellProcess;

/**
 * StatusProvider for shell-based containers spawned via
 * {@link ShellContainerProvider}. Runnable and configurable service.
 * 
 */
public class ShellStatusProvider implements StatusProviderService {

    @Override
    public boolean exists(String id) {
        Map<String, ShellProcess> processes = ShellContainerSingleton.getProcesses();

        synchronized (processes) {
            return processes.containsKey(id);
        }
    }

    @Override
    public boolean isHealthy(String id) {
        Map<String, ShellProcess> processes = ShellContainerSingleton.getProcesses();

        synchronized (processes) {
            ShellProcess shell = processes.get(id);

            if (shell == null)
                return false;

            if (!ShellUtils.hasMarker(shell.tmpDir))
                return false;

            try {
                // exit value
                shell.process.exitValue();
                return false;
            } catch (IllegalThreadStateException e) {
                // expected
            }

            return true;
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
