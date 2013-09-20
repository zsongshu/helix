package org.apache.helix.metamanager.bootstrapper;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.metamanager.Service;
import org.apache.helix.metamanager.bootstrap.BootUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class MetaService implements Service {

    static final Logger log = Logger.getLogger(MetaService.class);

    Service             service;

    String              clazz;
    String              metaAddress;
    
    String              metaCluster;

    Properties          config;

    @Override
    public void configure(Properties properties) throws Exception {
        clazz = properties.getProperty("class");
        metaAddress = properties.getProperty("metaaddress", "localhost:2199");
        metaCluster = properties.getProperty("metacluster", "metacluster");

        this.config = new Properties();
        this.config.putAll(properties);
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("starting service '%s' (config=%s)", clazz, config));

        HelixAdmin admin = new ZKHelixAdmin(metaAddress);
        
        HelixConfigScope managedScope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER, metaCluster).build();
        Map<String, String> managedProps = admin.getConfig(managedScope, Lists.newArrayList("cluster", "address"));
        config.putAll(managedProps);
        
        Collection<String> resources = admin.getResourcesInCluster(metaCluster);
        config.put("containers", StringUtils.join(resources, ","));
        
        for(String resource : admin.getResourcesInCluster(metaCluster)) {
            HelixConfigScope resScope = new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE, metaCluster, resource).build();
            List<String> resKeys = admin.getConfigKeys(resScope);
            Map<String, String> resProps = admin.getConfig(resScope, resKeys);
            
            for(Map.Entry<String, String> entry : resProps.entrySet()) {
                config.put(resource + "." + entry.getKey(), entry.getValue());
            }
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
