package org.apache.helix.autoscale.bootstrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.HelixAdmin;
import org.apache.helix.autoscale.Service;
import org.apache.helix.autoscale.provider.ProviderRebalancer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * Bootstrapping meta resource. Create container type configuration in Helix
 * zookeeper namespace.
 * 
 */
public class MetaResourceService implements Service {

    static final Logger log = Logger.getLogger(MetaResourceService.class);

    String              metaCluster;
    String              metaAddress;
    String              name;
    Map<String, String> config;

    @Override
    public void configure(Properties properties) throws Exception {
        metaCluster = properties.getProperty("metacluster", "metacluster");
        metaAddress = properties.getProperty("metaaddress", "localhost:2199");
        name = properties.getProperty("name", "container");

        this.config = new HashMap<String, String>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            this.config.put((String) entry.getKey(), (String) entry.getValue());
        }
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("setting up meta resource '%s' at '%s/%s'", name, metaAddress, metaCluster));
        HelixAdmin admin = new ZKHelixAdmin(metaAddress);

        log.info(String.format("setting up container '%s' (config='%s')", name, config));

        admin.addResource(metaCluster, name, 1, "OnlineOffline", RebalanceMode.USER_DEFINED.toString());
        IdealState idealState = admin.getResourceIdealState(metaCluster, name);
        idealState.setRebalancerClassName(ProviderRebalancer.class.getName());
        idealState.setReplicas("1");

        // BEGIN workaround
        // FIXME workaround for HELIX-226
        Map<String, List<String>> listFields = Maps.newHashMap();
        Map<String, Map<String, String>> mapFields = Maps.newHashMap();
        for (int i = 0; i < 256; i++) {
            String partitionName = name + "_" + i;
            listFields.put(partitionName, new ArrayList<String>());
            mapFields.put(partitionName, new HashMap<String, String>());
        }
        idealState.getRecord().setListFields(listFields);
        idealState.getRecord().setMapFields(mapFields);
        // END workaround

        admin.setResourceIdealState(metaCluster, name, idealState);

        HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE, metaCluster, name).build();
        admin.setConfig(scope, this.config);

        admin.close();
        log.info("setup complete");
    }

    @Override
    public void stop() throws Exception {
        // left blank
    }

}
