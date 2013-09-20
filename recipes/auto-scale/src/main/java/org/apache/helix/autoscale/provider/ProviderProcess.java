package org.apache.helix.autoscale.provider;

import java.util.Properties;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.autoscale.ClusterAdmin;
import org.apache.helix.autoscale.ContainerProvider;
import org.apache.helix.autoscale.HelixClusterAdmin;
import org.apache.helix.autoscale.Service;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Helix participant for ContainerProvider. Configurable via ProviderProperties
 * and runnable service.
 * 
 */
public class ProviderProcess implements Service {
    static final Logger log = Logger.getLogger(ProviderProcess.class);

    ClusterAdmin        admin;

    ProviderProperties  properties;
    ContainerProvider   provider;
    HelixAdmin          helixAdmin;
    HelixManager        participantManager;

    @Override
    public void configure(Properties properties) throws Exception {
        Preconditions.checkNotNull(properties);
        ProviderProperties providerProperties = new ProviderProperties();
        providerProperties.putAll(properties);
        Preconditions.checkArgument(providerProperties.isValid());

        this.properties = providerProperties;

    }

    public void setConteinerProvider(ContainerProvider provider) {
        this.provider = provider;
    }

    @Override
    public void start() throws Exception {
        Preconditions.checkNotNull(provider);

        log.info(String.format("Registering provider '%s' at '%s/%s'", properties.getName(), properties.getMetaAddress(), properties.getMetaCluster()));
        HelixAdmin metaHelixAdmin = new ZKHelixAdmin(properties.getMetaAddress());
        metaHelixAdmin.addInstance(properties.getMetaCluster(), new InstanceConfig(properties.getName()));
        metaHelixAdmin.close();

        log.info(String.format("Starting provider '%s'", properties.getName()));
        helixAdmin = new ZKHelixAdmin(properties.getAddress());
        admin = new HelixClusterAdmin(properties.getCluster(), helixAdmin);

        participantManager = HelixManagerFactory.getZKHelixManager(properties.getMetaCluster(), properties.getName(), InstanceType.PARTICIPANT,
                properties.getMetaAddress());
        participantManager.getStateMachineEngine().registerStateModelFactory("OnlineOffline", new ProviderStateModelFactory(provider, admin));
        participantManager.connect();

        log.info(String.format("Successfully started provider '%s'", properties.getName()));
    }

    @Override
    public void stop() {
        log.info(String.format("Stopping provider '%s'", properties.getName()));
        if (participantManager != null) {
            participantManager.disconnect();
            participantManager = null;
        }
        if (helixAdmin != null) {
            helixAdmin.close();
            helixAdmin = null;
        }
    }
}
