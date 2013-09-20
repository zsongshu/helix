package org.apache.helix.metamanager.impl.yarn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.metamanager.Service;
import org.apache.log4j.Logger;

/**
 * Configurable and runnable service for {@link YarnDataProvider} based on
 * zookeeper.
 * 
 */
public class ZookeeperYarnDataProvider implements YarnDataProvider, Service {

    static final Logger log                 = Logger.getLogger(ZookeeperYarnDataProvider.class);

    static final String CONTAINER_NAMESPACE = "containers";

    static final String BASE_PATH           = "/" + CONTAINER_NAMESPACE;

    static final int    META_TIMEOUT        = 5000;
    static final long   POLL_INTERVAL       = 100;

    String              yarndata;

    ZkClient            client;

    public ZookeeperYarnDataProvider() {
        // left blank
    }

    public ZookeeperYarnDataProvider(String yarndataAddress) {
        this.yarndata = yarndataAddress;
    }

    @Override
    public void configure(Properties properties) throws Exception {
        this.yarndata = properties.getProperty("yarndata");
    }

    @Override
    public void start() {
        log.debug(String.format("starting yarndata service for '%s'", yarndata));

        client = new ZkClient(yarndata, META_TIMEOUT, META_TIMEOUT);

        client.createPersistent(BASE_PATH, true);
    }

    @Override
    public void stop() {
        log.debug(String.format("stopping yarndata service for '%s'", yarndata));
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public boolean exists(String id) {
        return client.exists(makePath(id));
    }

    @Override
    public void create(YarnContainerData meta) throws Exception {
        client.createEphemeral(makePath(meta.id), YarnUtils.toJson(meta));
    }

    @Override
    public YarnContainerData read(String id) throws Exception {
        return YarnUtils.fromJson(client.<String> readData(makePath(id)));
    }

    @Override
    public Collection<YarnContainerData> readAll() throws Exception {
        Collection<YarnContainerData> yarndata = new ArrayList<YarnContainerData>();
        for (String id : client.getChildren(BASE_PATH)) {
            yarndata.add(YarnUtils.fromJson(client.<String> readData(makePath(id))));
        }
        return yarndata;
    }

    @Override
    public void update(YarnContainerData meta) throws Exception {
        client.writeData(makePath(meta.id), YarnUtils.toJson(meta));
    }

    @Override
    public void delete(String id) throws Exception {
        client.delete(makePath(id));
    }

    String makePath(String containerId) {
        return BASE_PATH + "/" + containerId;
    }

}