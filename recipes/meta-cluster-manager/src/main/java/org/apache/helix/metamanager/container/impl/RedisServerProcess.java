package org.apache.helix.metamanager.container.impl;

import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.NotificationContext;
import org.apache.helix.metamanager.container.ContainerProcess;
import org.apache.helix.metamanager.container.ContainerProcessProperties;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

public class RedisServerProcess extends ContainerProcess {

    static final Logger        log                  = Logger.getLogger(RedisServerProcess.class);

    public static final String REDIS_SERVER_COMMAND = "redis-server";

    public static final long   MONITOR_INTERVAL     = 5000;

    ZkClient                   zookeeper;

    final String               address;
    final String               root;
    final String               name;
    final int                  basePort;

    Process                    process;

    ScheduledExecutorService   executor;

    public RedisServerProcess(ContainerProcessProperties properties) {
        super(properties);

        setModelName("OnlineOffline");
        setModelFactory(new RedisServerModelFactory());

        address = properties.getProperty("address");
        root = properties.getProperty("root");
        basePort = Integer.valueOf(properties.getProperty("baseport"));
        name = properties.getProperty(ContainerProcessProperties.HELIX_INSTANCE);
    }

    @Override
    protected void startContainer() throws Exception {
        log.info(String.format("starting redis server container for instance '%s'", name));

        String hostname = InetAddress.getLocalHost().getHostName();
        int port = basePort + Integer.valueOf(name.split("_")[1]);

        log.debug(String.format("Starting redis server at '%s:%d'", hostname, port));

        ProcessBuilder builder = new ProcessBuilder();
        builder.command(REDIS_SERVER_COMMAND, "--port", String.valueOf(port));
        process = builder.start();

        log.debug("Updating zookeeper");
        zookeeper = new ZkClient(address);
        zookeeper.deleteRecursive("/" + root + "/" + name);
        zookeeper.createPersistent("/" + root + "/" + name, true);
        zookeeper.createPersistent("/" + root + "/" + name + "/hostname", hostname);
        zookeeper.createPersistent("/" + root + "/" + name + "/port", String.valueOf(port));

        log.debug("Starting process monitor");
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new ProcessMonitor(), 0, MONITOR_INTERVAL, TimeUnit.MILLISECONDS);

    }

    @Override
    protected void stopContainer() throws Exception {
        log.info("stopping redis server container");

        log.debug("Stopping process monitor");
        executor.shutdownNow();

        log.debug("Updating zookeeper");
        zookeeper.deleteRecursive("/" + root + "/" + name);
        zookeeper.close();

        log.debug("Stopping process");
        process.destroy();
        process.waitFor();
    }

    public class RedisServerModelFactory extends StateModelFactory<RedisServerModel> {
        @Override
        public RedisServerModel createNewStateModel(String partitionName) {
            return new RedisServerModel();
        }
    }

    @StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "ONLINE", "DROPPED" })
    public class RedisServerModel extends StateModel {

        @Transition(from = "OFFLINE", to = "ONLINE")
        public void offlineToSlave(Message m, NotificationContext context) {
            // left blank
            log.trace(String.format("%s transitioning from %s to %s", context.getManager().getInstanceName(), m.getFromState(), m.getToState()));
        }

        @Transition(from = "ONLINE", to = "OFFLINE")
        public void slaveToOffline(Message m, NotificationContext context) {
            // left blank
            log.trace(String.format("%s transitioning from %s to %s", context.getManager().getInstanceName(), m.getFromState(), m.getToState()));
        }

        @Transition(from = "OFFLINE", to = "DROPPED")
        public void offlineToDropped(Message m, NotificationContext context) {
            // left blank
            log.trace(String.format("%s transitioning from %s to %s", context.getManager().getInstanceName(), m.getFromState(), m.getToState()));
        }

    }

    private class ProcessMonitor implements Runnable {
        @Override
        public void run() {
            try {
                process.exitValue();
                log.warn("detected process failure");
                fail();
            } catch (Exception e) {
                // expected
            }
        }
    }

}
