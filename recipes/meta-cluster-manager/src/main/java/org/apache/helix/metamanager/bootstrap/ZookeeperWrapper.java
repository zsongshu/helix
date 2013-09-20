package org.apache.helix.metamanager.bootstrap;

import java.io.File;
import java.util.Properties;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class ZookeeperWrapper {

    static final Logger log = Logger.getLogger(ZookeeperWrapper.class);

    ZkServer            server;
    Properties          properties;

    public ZookeeperWrapper(Properties properties) {
        this.properties = properties;
    }

    public void startService() {
        String dataDir = properties.getProperty("datadir");
        String logDir = properties.getProperty("logdir");
        int port = Integer.parseInt(properties.getProperty("port"));

        log.info(String.format("starting zookeeper service (dataDir='%s', logDir='%s', port=%d)", dataDir, logDir, port));

        FileUtils.deleteQuietly(new File(dataDir));
        FileUtils.deleteQuietly(new File(logDir));

        IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
            @Override
            public void createDefaultNameSpace(ZkClient zkClient) {
                // left blank
            }
        };

        server = new ZkServer(dataDir, logDir, defaultNameSpace, port);
        server.start();
    }

    public void stopService() {
        log.info("stopping zookeeper service");

        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    public ZkServer getZookeeper() {
        return server;
    }

}
