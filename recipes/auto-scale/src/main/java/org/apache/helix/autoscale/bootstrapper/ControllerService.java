package org.apache.helix.autoscale.bootstrapper;

import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.helix.HelixManager;
import org.apache.helix.autoscale.Service;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.log4j.Logger;

/**
 * Helix controller bootstrapping and management. Create standalone controller
 * for managed Helix cluster.
 * 
 */
public class ControllerService implements Service {

    static final Logger      log = Logger.getLogger(ControllerService.class);

    String                   name;
    String                   cluster;
    String                   address;

    HelixManager             manager;

    ScheduledExecutorService executor;

    @Override
    public void configure(Properties properties) throws Exception {
        name = properties.getProperty("name", "controller");
        cluster = properties.getProperty("cluster", "cluster");
        address = properties.getProperty("address", "localhost:2199");
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("starting controller '%s' at '%s/%s'", name, address, cluster));
        manager = HelixControllerMain.startHelixController(address, cluster, name, HelixControllerMain.STANDALONE);
    }

    @Override
    public void stop() throws Exception {
        if (manager != null) {
            log.info(String.format("stopping controller '%s' at '%s/%s'", name, address, cluster));
            manager.disconnect();
            manager = null;
        }
    }

}
