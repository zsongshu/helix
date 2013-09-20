package org.apache.helix.autoscale.impl.shell;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.helix.autoscale.container.ContainerProcess;
import org.apache.helix.autoscale.container.ContainerProcessProperties;
import org.apache.helix.autoscale.container.ContainerUtils;
import org.apache.log4j.Logger;

/**
 * Host process for Shell-based container. ContainerProcess configuration is
 * read from path in first command-line argument. Status is maintained using
 * temporary marker file. (Program entry point)
 * 
 */
class ShellContainerProcess {
    static final Logger             log              = Logger.getLogger(ShellContainerProcess.class);

    public static final long        MONITOR_INTERVAL = 5000;

    static String                   markerDir;
    static ContainerProcess         process;
    static ScheduledExecutorService executor         = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) throws Exception {
        final String propertiesPath = args[0];
        markerDir = args[1];

        ContainerProcessProperties properties = ContainerUtils.getPropertiesFromPath(propertiesPath);

        process = ContainerUtils.createProcess(properties);

        log.debug("Installing shutdown hooks");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.debug("Running shutdown hook");
                try {
                    ShellContainerProcess.stop();
                } catch (Exception ignore) {
                }
            }
        });

        log.debug("Launching shell container process");
        process.start();

        ShellUtils.createMarker(new File(markerDir));

        log.debug("Launching process monitor");
        executor.scheduleAtFixedRate(new ProcessMonitor(), 0, MONITOR_INTERVAL, TimeUnit.MILLISECONDS);
    }

    static void stop() throws InterruptedException {
        log.debug("Shutting down shell process");
        if (process != null) {
            process.stop();
            ShellUtils.destroyMarker(new File(markerDir));
        }
        if (executor != null) {
            executor.shutdownNow();
            while (!executor.isTerminated()) {
                Thread.sleep(100);
            }
            executor = null;
        }
    }

    static class ProcessMonitor implements Runnable {
        @Override
        public void run() {
            if (process.isFailed()) {
                log.warn("detected process failure");
                try {
                    ShellContainerProcess.stop();
                } catch (Exception ignore) {
                }
                System.exit(1);
            }
            if (!process.isActive()) {
                log.warn("detected process shutdown");
                try {
                    ShellContainerProcess.stop();
                } catch (Exception ignore) {
                }
            }
        }
    }

}
