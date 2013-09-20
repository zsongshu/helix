package org.apache.helix.autoscale.impl.yarn;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.helix.autoscale.Service;
import org.apache.helix.autoscale.container.ContainerProcess;
import org.apache.helix.autoscale.container.ContainerProcessProperties;
import org.apache.helix.autoscale.container.ContainerUtils;
import org.apache.helix.autoscale.impl.yarn.YarnContainerData.ContainerState;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Configurable and runnable service for YARN-based containers. Continuously
 * checks container meta data and process state and triggers state changes and
 * container setup and shutdown.
 * 
 */
class YarnContainerService implements Service {
    static final Logger            log                       = Logger.getLogger(YarnContainerService.class);

    static final long              CONTAINERSERVICE_INTERVAL = 1000;

    YarnContainerProcessProperties properties;

    YarnDataProvider               metaService;
    ScheduledExecutorService       executor;

    ContainerProcess               process;

    @Override
    public void configure(Properties properties) throws Exception {
        Preconditions.checkNotNull(properties);
        YarnContainerProcessProperties containerProperties = new YarnContainerProcessProperties();
        containerProperties.putAll(properties);
        Preconditions.checkArgument(containerProperties.isValid());

        this.properties = containerProperties;
    }

    public void setYarnDataProvider(YarnDataProvider metaService) {
        this.metaService = metaService;
    }

    @Override
    public void start() {
        Preconditions.checkNotNull(metaService);
        Preconditions.checkNotNull(properties);
        Preconditions.checkState(properties.isValid());

        log.debug("starting yarn container service");

        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new ContainerStatusService(), 0, CONTAINERSERVICE_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        log.debug("stopping yarn container service");

        if (executor != null) {
            executor.shutdown();
            while (!executor.isTerminated()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            executor = null;
        }

        destroyLocalContainerNamespace();
    }

    class ContainerStatusService implements Runnable {
        @Override
        public void run() {
            log.info("updating container status");

            try {
                if (!metaService.exists(properties.getName())) {
                    log.warn(String.format("YarnData for '%s' does not exist. Terminating yarn service.", properties.getName()));
                    process.stop();
                    stop();
                }

                YarnContainerData meta = metaService.read(properties.getName());

                if (meta.state == ContainerState.CONNECTING) {
                    log.trace("container connecting");
                    try {
                        ContainerProcessProperties containerProperties = meta.getProperties();

                        containerProperties.setProperty(ContainerProcessProperties.CLUSTER, properties.getCluster());
                        containerProperties.setProperty(ContainerProcessProperties.ADDRESS, properties.getAddress());
                        containerProperties.setProperty(ContainerProcessProperties.NAME, properties.getName());

                        process = ContainerUtils.createProcess(containerProperties);
                        process.start();
                    } catch (Exception e) {
                        log.error("Failed to start participant, going to failed", e);
                    }

                    if (process.isActive()) {
                        log.trace("process active, activating container");
                        metaService.update(meta.setState(ContainerState.ACTIVE));

                    } else if (process.isFailed()) {
                        log.trace("process failed, failing container");
                        metaService.update(meta.setState(ContainerState.FAILED));

                    } else {
                        log.trace("process state unknown, failing container");
                        metaService.update(meta.setState(ContainerState.FAILED));
                    }
                }

                if (meta.state == ContainerState.ACTIVE) {
                    log.trace("container active");
                    if (process.isFailed()) {
                        log.trace("process failed, failing container");
                        metaService.update(meta.setState(ContainerState.FAILED));

                    } else if (!process.isActive()) {
                        log.trace("process not active, halting container");
                        process.stop();
                        metaService.update(meta.setState(ContainerState.HALTED));
                    }
                }

                if (meta.state == ContainerState.TEARDOWN) {
                    log.trace("container teardown");
                    process.stop();
                    metaService.update(meta.setState(ContainerState.HALTED));
                }

            } catch (Exception e) {
                log.error(String.format("Error while updating container '%s' status", properties.getName()), e);
            }
        }
    }

    public static void destroyLocalContainerNamespace() {
        log.info("cleaning up container directory");
        FileUtils.deleteQuietly(new File(YarnUtils.YARN_CONTAINER_DESTINATION));
        FileUtils.deleteQuietly(new File(YarnUtils.YARN_CONTAINER_PROPERTIES));
    }

}
