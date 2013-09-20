package org.apache.helix.metamanager.impl.yarn;

import java.util.ArrayList;
import java.util.Collection;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.log4j.Logger;

public class ZookeeperMetadataService implements MetadataService {

    static final Logger log                 = Logger.getLogger(ZookeeperMetadataService.class);

    static final String CONTAINER_NAMESPACE = "containers";

    static final String BASE_PATH           = "/" + CONTAINER_NAMESPACE;

    static final long   POLL_INTERVAL       = 100;

    final String        metadataAddress;

    ZkClient            client;

    public ZookeeperMetadataService(String metadataAddress) {
        this.metadataAddress = metadataAddress;
    }

    public void startService() {
        log.debug(String.format("starting metadata service for '%s'", metadataAddress));

        client = new ZkClient(metadataAddress);

        client.createPersistent(BASE_PATH, true);
    }

    public void stopService() {
        log.debug(String.format("stopping metadata service for '%s'", metadataAddress));
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
    public void create(ContainerMetadata meta) throws MetadataServiceException {
        try {
            client.createPersistent(makePath(meta.id), YarnUtils.toJson(meta));
        } catch (ZkException e) {
            throw new MetadataServiceException(e);
        }
    }

    @Override
    public ContainerMetadata read(String id) throws MetadataServiceException {
        try {
            return YarnUtils.fromJson(client.<String> readData(makePath(id)));
        } catch (ZkException e) {
            throw new MetadataServiceException(e);
        }
    }

    @Override
    public Collection<ContainerMetadata> readAll() throws MetadataServiceException {
        try {
            Collection<ContainerMetadata> metadata = new ArrayList<ContainerMetadata>();
            for (String id : client.getChildren(BASE_PATH)) {
                metadata.add(YarnUtils.fromJson(client.<String> readData(makePath(id))));
            }
            return metadata;
        } catch (ZkException e) {
            throw new MetadataServiceException(e);
        }
    }

    @Override
    public void update(ContainerMetadata meta) throws MetadataServiceException {
        try {
            client.writeData(makePath(meta.id), YarnUtils.toJson(meta));
        } catch (ZkException e) {
            throw new MetadataServiceException(e);
        }
    }

    @Override
    public void delete(String id) throws MetadataServiceException {
        try {
            client.delete(makePath(id));
        } catch (ZkException e) {
            throw new MetadataServiceException(e);
        }
    }

    String makePath(String containerId) {
        return BASE_PATH + "/" + containerId;
    }

}