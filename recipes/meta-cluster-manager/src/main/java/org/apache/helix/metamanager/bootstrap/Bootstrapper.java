package org.apache.helix.metamanager.bootstrap;

import java.util.Properties;

import org.apache.log4j.Logger;

public class Bootstrapper {

    static final Logger log = Logger.getLogger(Bootstrapper.class);

    ManagedCluster      managed;
    MetaCluster         meta;
    ZookeeperWrapper    zookeeper;
    Properties          properties;

    public Bootstrapper(Properties properties) {
        this.properties = properties;
    }

    public void start() throws Exception {
        log.info("bootstrapping cluster");
        if (BootUtils.hasNamespace(properties, "zookeeper")) {
            log.info("starting zookeeper");
            zookeeper = new ZookeeperWrapper(BootUtils.getNamespace(properties, "zookeeper"));
            zookeeper.startService();
        }

        log.info("starting managed cluster");
        managed = new ManagedCluster();
        managed.setProperties(BootUtils.getNamespace(properties, "managed"));
        managed.start();

        log.info("starting meta cluster");
        meta = new MetaCluster();
        meta.setProperties(BootUtils.getNamespace(properties, "meta"));
        meta.start();
    }

    public void stop() throws Exception {
        log.info("tearing down cluster");
        if (meta != null) {
            log.info("stopping meta cluster");
            meta.stop();
            meta = null;
        }
        if (managed != null) {
            log.info("stopping managed cluster");
            managed.stop();
            managed = null;
        }
        if (zookeeper != null) {
            log.info("stopping zookeeper");
            zookeeper.stopService();
            zookeeper = null;
        }

    }

    public ManagedCluster getManaged() {
        return managed;
    }

    public MetaCluster getMeta() {
        return meta;
    }

    public ZookeeperWrapper getZookeeper() {
        return zookeeper;
    }

    public Properties getProperties() {
        return properties;
    }

    public static void main(String[] args) throws Exception {
        String resourcePath = args[0];

        log.info(String.format("reading cluster definition from '%s'", resourcePath));
        Properties properties = new Properties();
        properties.load(ClassLoader.getSystemResourceAsStream(resourcePath));

        final Bootstrapper boot = new Bootstrapper(properties);
        boot.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try { boot.stop(); } catch(Exception ignore) {}
            }
        }));
    }

}
