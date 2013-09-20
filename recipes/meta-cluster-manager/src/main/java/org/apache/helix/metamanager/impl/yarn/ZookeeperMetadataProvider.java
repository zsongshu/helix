package org.apache.helix.metamanager.impl.yarn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.helix.metamanager.Service;
import org.apache.log4j.Logger;

public class ZookeeperMetadataProvider implements MetadataProvider, Service {

    static final Logger log                 = Logger.getLogger(ZookeeperMetadataProvider.class);

    static final String CONTAINER_NAMESPACE = "containers";

    static final String BASE_PATH           = "/" + CONTAINER_NAMESPACE;

    static final int    META_TIMEOUT        = 5000;
    static final long   POLL_INTERVAL       = 100;

    String              metadata;

    ZkClient            client;

    public ZookeeperMetadataProvider() {
        // left blank
    }

    public ZookeeperMetadataProvider(String metadataAddress) {
        this.metadata = metadataAddress;
    }

    @Override
    public void configure(Properties properties) throws Exception {
        this.metadata = properties.getProperty("metadata");
    }

    @Override
    public void start() {
        log.debug(String.format("starting metadata service for '%s'", metadata));

        client = new ZkClient(metadata, META_TIMEOUT, META_TIMEOUT);

        client.createPersistent(BASE_PATH, true);
    }

    @Override
    public void stop() {
        log.debug(String.format("stopping metadata service for '%s'", metadata));
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
    public void create(ContainerMetadata meta) throws MetadataException {
        try {
            client.createEphemeral(makePath(meta.id), YarnUtils.toJson(meta));
        } catch (ZkException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public ContainerMetadata read(String id) throws MetadataException {
        try {
            return YarnUtils.fromJson(client.<String> readData(makePath(id)));
        } catch (ZkException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Collection<ContainerMetadata> readAll() throws MetadataException {
        try {
            Collection<ContainerMetadata> metadata = new ArrayList<ContainerMetadata>();
            for (String id : client.getChildren(BASE_PATH)) {
                metadata.add(YarnUtils.fromJson(client.<String> readData(makePath(id))));
            }
            return metadata;
        } catch (ZkException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void update(ContainerMetadata meta) throws MetadataException {
        try {
            client.writeData(makePath(meta.id), YarnUtils.toJson(meta));
        } catch (ZkException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void delete(String id) throws MetadataException {
        try {
            client.delete(makePath(id));
        } catch (ZkException e) {
            throw new MetadataException(e);
        }
    }

    String makePath(String containerId) {
        return BASE_PATH + "/" + containerId;
    }

}