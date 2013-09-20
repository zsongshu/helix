package org.apache.helix.metamanager.bootstrapper;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.metamanager.Service;
import org.apache.helix.metamanager.provider.ProviderProperties;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * ContainerProvider bootstrapping and management. Create container provider
 * participant, configure with container properties from meta resources and
 * connect to meta cluster.
 * 
 */
public class MetaProviderService implements Service {

    static final Logger log = Logger.getLogger(MetaProviderService.class);

    Service             service;

    String              clazz;
    String              metaAddress;
    String              metaCluster;

    ProviderProperties  config;

    @Override
    public void configure(Properties properties) throws Exception {
        clazz = properties.getProperty("class");
        metaAddress = properties.getProperty("metaaddress", "localhost:2199");
        metaCluster = properties.getProperty("metacluster", "metacluster");

        config = new ProviderProperties();
        config.putAll(properties);
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("starting service '%s' (config=%s)", clazz, config));

        HelixAdmin admin = new ZKHelixAdmin(metaAddress);

        HelixConfigScope managedScope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER, metaCluster).build();
        Map<String, String> managedProps = admin.getConfig(managedScope, Lists.newArrayList("cluster", "address"));
        config.putAll(managedProps);

        for (String resource : admin.getResourcesInCluster(metaCluster)) {
            HelixConfigScope resScope = new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE, metaCluster, resource).build();
            List<String> resKeys = admin.getConfigKeys(resScope);
            Map<String, String> resProps = admin.getConfig(resScope, resKeys);

            Properties properties = new Properties();
            properties.putAll(resProps);

            config.addContainer(resource, properties);
        }

        service = BootUtils.createInstance(clazz);
        service.configure(config);
        service.start();
    }

    @Override
    public void stop() throws Exception {
        log.info(String.format("stopping service '%s' (config=%s)", clazz, config));
        if (service != null) {
            service.stop();
            service = null;
        }
    }

}
