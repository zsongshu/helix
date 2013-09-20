package org.apache.helix.metamanager.bootstrap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.metamanager.ConfigTool;
import org.apache.helix.metamanager.provider.ProviderProcess;
import org.apache.helix.metamanager.provider.ProviderRebalancer;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class MetaCluster {

    static final Logger         log              = Logger.getLogger(MetaCluster.class);

    private static final String DEFAULT_CLUSTER = "meta";
    private static final String DEFAULT_MANAGED = "managed";
    private static final String DEFAULT_INTERVAL = "10000";

    Properties                  properties;

    TargetWrapper               target;
    StatusWrapper               status;
    ProviderWrapper             provider;

    HelixAdmin                  admin;
    HelixManager                controllerManager;
    ProviderProcess             providerProcess;

    String                      cluster;
    String                      address;
    String                      managed;
    int                         interval;

    ScheduledExecutorService    executor;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void start() throws Exception {
        Preconditions.checkArgument(BootUtils.hasNamespace(properties, "target"), "No 'target' property specified");
        Preconditions.checkArgument(BootUtils.hasNamespace(properties, "status"), "No 'status' property specified");
        Preconditions.checkArgument(BootUtils.hasNamespace(properties, "provider"), "No 'provider' property specified");

        cluster = properties.getProperty("cluster", DEFAULT_CLUSTER);
        address = properties.getProperty("address");
        managed = properties.getProperty("managed", DEFAULT_MANAGED);
        interval = Integer.valueOf(properties.getProperty("interval", DEFAULT_INTERVAL));

        log.info(String.format("starting meta cluster service (cluster='%s', address='%s', managed='%s')", cluster, address, managed));

        log.debug("setting up cluster admin");
        admin = new ZKHelixAdmin(address);
        admin.addCluster(cluster, false);
        admin.addStateModelDef(cluster, "OnlineOffline", new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));

        log.debug("setting up target service");
        target = new TargetWrapper(BootUtils.getNamespace(properties, "target"));
        target.startService();

        log.debug("setting up container status service");
        status = new StatusWrapper(BootUtils.getNamespace(properties, "status"));
        status.startService();

        log.debug("setting up container provider service");
        provider = new ProviderWrapper(BootUtils.getNamespace(properties, "provider"));
        admin.addInstance(cluster, new InstanceConfig(provider.getProviderName()));
        
        provider.startService();

        log.debug("setting up config tool");
        ConfigTool.setTargetProvider(target.getTarget());
        ConfigTool.setStatusProvider(status.getStatus());

        log.debug("setting up provider");
        String providerName = provider.getProviderName();

        admin.addInstance(cluster, new InstanceConfig(providerName));

        for (String containerType : provider.getContainerTypes()) {
            log.debug(String.format("setting up container type '%s'", containerType));

            admin.addResource(cluster, containerType, target.getTarget().getTargetContainerCount(containerType), "OnlineOffline",
                    RebalanceMode.USER_DEFINED.toString());

            IdealState idealState = admin.getResourceIdealState(cluster, containerType);
            idealState.setRebalancerClassName(ProviderRebalancer.class.getName());
            idealState.setReplicas("1");
            
            // BEGIN workaround
            // FIXME workaround for HELIX-226
            Map<String, List<String>> listFields = Maps.newHashMap();
            Map<String, Map<String, String>> mapFields = Maps.newHashMap();
            for(int i=0; i<256; i++) {
                String partitionName = containerType + "_" + i;
                listFields.put(partitionName, new ArrayList<String>());
                mapFields.put(partitionName, new HashMap<String, String>());
            }
            idealState.getRecord().setListFields(listFields);
            idealState.getRecord().setMapFields(mapFields);
            // END workaround
            
            admin.setResourceIdealState(cluster, containerType, idealState);
        }

        log.debug("starting controller");
        controllerManager = HelixControllerMain.startHelixController(address, cluster, "metaController", HelixControllerMain.STANDALONE);

        log.debug("starting state refresh service");
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new MetaRefreshRunnable(), interval, interval, TimeUnit.MILLISECONDS);
        
        HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER, cluster).build();
        admin.setConfig(scope, Collections.singletonMap("key", "value"));
        
    }

    public void stop() throws Exception {
        log.info("stopping meta cluster service");
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        if (controllerManager != null) {
            controllerManager.disconnect();
            controllerManager = null;
        }
        if (providerProcess != null) {
            providerProcess.stop();
            providerProcess = null;
        }
        if (provider != null) {
            provider.stopService();
            provider = null;
        }
        if (status != null) {
            status.stopService();
            status = null;
        }
        if (target != null) {
            target.stopService();
            target = null;
        }
        if (admin != null) {
            admin.close();
            admin = null;
        }
    }

    public TargetWrapper getTarget() {
        return target;
    }

    public StatusWrapper getStatus() {
        return status;
    }

    public ProviderWrapper getProvider() {
        return provider;
    }

    private class MetaRefreshRunnable implements Runnable {
        @Override
        public void run() {
            log.debug("running status refresh");
            for (String containerType : provider.getContainerTypes()) {
                log.debug(String.format("refreshing container type '%s'", containerType));

                IdealState poke = admin.getResourceIdealState(cluster, containerType);
                admin.setResourceIdealState(cluster, containerType, poke);
            }
        }
    }

}
