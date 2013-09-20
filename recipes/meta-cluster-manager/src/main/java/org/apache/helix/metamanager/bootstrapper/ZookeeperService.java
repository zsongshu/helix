package org.apache.helix.metamanager.bootstrapper;

import java.io.File;
import java.util.Properties;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.helix.metamanager.Service;
import org.apache.log4j.Logger;

/**
 * Bootstrapping zookeeper. Convenience tool for creating standalone zookeeper
 * instance for test deployments. For production use a separate zookeeper
 * cluster is strongly recommended.
 * 
 */
public class ZookeeperService implements Service {

    static final Logger log = Logger.getLogger(ZookeeperService.class);

    String              dataDir;
    String              logDir;
    int                 port;

    ZkServer            server;

    @Override
    public void configure(Properties properties) throws Exception {
        dataDir = properties.getProperty("datadir", "/tmp/zk/data");
        logDir = properties.getProperty("logdir", "/tmp/zk/log");
        port = Integer.parseInt(properties.getProperty("port", "2199"));
    }

    @Override
    public void start() {
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

    @Override
    public void stop() {
        log.info("stopping zookeeper service");

        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

}
