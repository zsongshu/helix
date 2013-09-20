package org.apache.helix.metamanager.bootstrapper;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.metamanager.Service;
import org.apache.helix.metamanager.StatusProviderService;
import org.apache.helix.metamanager.TargetProviderService;
import org.apache.helix.metamanager.provider.ProviderRebalancerSingleton;
import org.apache.helix.model.IdealState;
import org.apache.log4j.Logger;

/**
 * Meta cluster controller bootstrapping and management. Create standalone
 * controller for Helix meta cluster. Spawn StatusProvider and TargetProvider
 * and trigger periodic status refresh in meta cluster.
 * 
 */
public class MetaControllerService implements Service {

    static final Logger      log = Logger.getLogger(MetaControllerService.class);

    String                   name;
    String                   metacluster;
    String                   metaaddress;
    long                     autorefresh;

    HelixManager             manager;
    StatusProviderService    statusService;
    TargetProviderService    targetService;
    ScheduledExecutorService executor;

    @Override
    public void configure(Properties properties) throws Exception {
        name = properties.getProperty("name", "controller");
        metacluster = properties.getProperty("metacluster", "metacluster");
        metaaddress = properties.getProperty("metaaddress", "localhost:2199");
        autorefresh = Long.valueOf(properties.getProperty("autorefresh", "0"));

        Properties statusProperties = BootUtils.getNamespace(properties, "status");
        statusService = BootUtils.createInstance(Class.forName(statusProperties.getProperty("class")));
        statusService.configure(statusProperties);
        ProviderRebalancerSingleton.setStatusProvider(statusService);

        Properties targetProperties = BootUtils.getNamespace(properties, "target");
        targetService = BootUtils.createInstance(Class.forName(targetProperties.getProperty("class")));
        targetService.configure(targetProperties);
        ProviderRebalancerSingleton.setTargetProvider(targetService);
    }

    @Override
    public void start() throws Exception {
        log.debug("Starting status service");
        statusService.start();

        log.debug("Starting target service");
        targetService.start();

        log.info(String.format("starting controller '%s' at '%s/%s'", name, metaaddress, metacluster));
        manager = HelixControllerMain.startHelixController(metaaddress, metacluster, name, HelixControllerMain.STANDALONE);

        if (autorefresh > 0) {
            log.debug(String.format("installing autorefresh with interval %d ms", autorefresh));
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(new RefreshRunnable(), autorefresh, autorefresh, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stop() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            while (!executor.isTerminated()) {
                Thread.sleep(100);
            }
            executor = null;
        }
        if (manager != null) {
            log.info(String.format("Stopping controller '%s' at '%s/%s'", name, metaaddress, metacluster));
            manager.disconnect();
            manager = null;
        }
        if (targetService != null) {
            log.debug("Stopping target service");
            targetService.stop();
            targetService = null;
        }
        if (statusService != null) {
            log.debug("Stopping status service");
            statusService.stop();
            statusService = null;
        }
    }

    private class RefreshRunnable implements Runnable {
        @Override
        public void run() {
            log.debug("running status refresh");
            HelixAdmin admin = manager.getClusterManagmentTool();

            for (String metaResource : admin.getResourcesInCluster(metacluster)) {
                log.debug(String.format("refreshing meta resource '%s'", metaResource));

                IdealState poke = admin.getResourceIdealState(metacluster, metaResource);
                admin.setResourceIdealState(metacluster, metaResource, poke);
            }
        }
    }
}
