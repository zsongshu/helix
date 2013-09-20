package org.apache.helix.autoscale;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.autoscale.Service;
import org.apache.helix.autoscale.StatusProvider;
import org.apache.helix.autoscale.StatusProviderService;
import org.apache.helix.autoscale.TargetProvider;
import org.apache.helix.autoscale.TargetProviderService;
import org.apache.helix.autoscale.impl.local.LocalContainerProviderProcess;
import org.apache.helix.autoscale.impl.shell.ShellContainerProviderProcess;
import org.apache.helix.autoscale.impl.yarn.YarnContainerProviderProcess;
import org.apache.helix.autoscale.impl.yarn.YarnContainerProviderProperties;
import org.apache.helix.autoscale.provider.ProviderProperties;
import org.apache.helix.autoscale.provider.ProviderRebalancer;
import org.apache.helix.autoscale.provider.ProviderRebalancerSingleton;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Utility for creating a test cluster without the bootstrapping tool. Methods
 * for verifying the number of active instances and partitions in a cluster.
 * 
 */
public class TestUtils {

    static final Logger                    log                      = Logger.getLogger(TestUtils.class);

    public static int                      zkPort;
    public static String                   zkAddress;
    public static String                   resmanAddress;
    public static String                   schedulerAddress;
    public static String                   hdfsAddress;
    public static String                   yarnUser;

    public static final String             metaClusterName          = "meta-cluster";
    public static final String             managedClusterName       = "managed-cluster";
    public static final String             metaResourceName         = "container";
    public static final String             managedResourceName      = "database";

    public static final int                numManagedPartitions     = 10;
    public static final int                numManagedReplica        = 2;

    public static final long               TEST_TIMEOUT             = 120000;
    public static final long               REBALANCE_TIMEOUT        = 60000;
    public static final long               POLL_INTERVAL            = 1000;

    public static final ProviderProperties providerProperties       = new ProviderProperties();

    public static ZkServer                 server                   = null;
    public static HelixAdmin               admin                    = null;
    public static HelixManager             metaControllerManager    = null;
    public static HelixManager             managedControllerManager = null;

    public static Collection<Service>      providerServices         = new ArrayList<Service>();
    public static Collection<Service>      auxServices              = new ArrayList<Service>();

    public static TargetProvider           targetProvider           = null;
    public static StatusProvider           statusProvider           = null;

    static {
        try {
            configure();
        } catch(Exception e) {
            log.error("Could not setup TestUtils", e);
            throw new RuntimeException(e);
        }
    }
    
    private TestUtils() {
        // left blank
    }
    
    public static void configure() throws IOException {
        configure("standalone.properties");
    }
    
    public static void configure(String resourcePath) throws IOException {
        log.info(String.format("Configuring Test cluster from %s", resourcePath));
        Properties properties = new Properties();
        properties.load(ClassLoader.getSystemResourceAsStream(resourcePath));
        configure(properties);
    }

    public static void configure(Properties properties) {
        log.info(String.format("Configuring from properties '%s'", properties));
        
        zkPort = Integer.valueOf(properties.getProperty("zookeeper.port"));
        zkAddress = properties.getProperty("zookeeper.address");
        resmanAddress = properties.getProperty("yarn.resourcemanager");
        schedulerAddress = properties.getProperty("yarn.scheduler");
        hdfsAddress = properties.getProperty("yarn.hdfs");
        yarnUser = properties.getProperty("yarn.user");
        
        Preconditions.checkNotNull(zkPort);
        Preconditions.checkNotNull(zkAddress);
        Preconditions.checkNotNull(resmanAddress);
        Preconditions.checkNotNull(schedulerAddress);
        Preconditions.checkNotNull(hdfsAddress);
        Preconditions.checkNotNull(yarnUser);
        
        configureInternal();
    }
    
    static void configureInternal() {
        providerProperties.clear();
        providerProperties.setProperty(ProviderProperties.ADDRESS, zkAddress);
        providerProperties.setProperty(ProviderProperties.CLUSTER, managedClusterName);
        providerProperties.setProperty(ProviderProperties.METAADDRESS, zkAddress);
        providerProperties.setProperty(ProviderProperties.METACLUSTER, metaClusterName);
        providerProperties.setProperty(ProviderProperties.NAME, "<unknown>");
    
        Properties containerProperties = new Properties();
        containerProperties.setProperty("class", "org.apache.helix.autoscale.impl.container.DummyMasterSlaveProcess");
    
        providerProperties.addContainer("container", containerProperties);
    
        log.info(String.format("Using provider properties '%s'", providerProperties));
    }

    public static void startZookeeper() throws Exception {
        log.info("Starting ZooKeeper");

        if (server != null)
            throw new IllegalStateException("Zookeeper already running");

        server = createLocalZookeeper();
        server.start();
    }

    public static void stopZookeeper() throws Exception {
        log.info("Stopping ZooKeeper");

        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    public static void startTestCluster(TargetProviderService targetProvider, StatusProviderService statusProvider, Service... containerProviderProcesses)
            throws Exception {
        log.debug(String.format("Starting test cluster"));

        if (server == null)
            throw new IllegalStateException("Zookeeper not running yet");

        if (!auxServices.isEmpty() || !providerServices.isEmpty() || admin != null || metaControllerManager != null || managedControllerManager != null)
            throw new IllegalStateException("TestCluster already running");

        log.debug("Create admin");
        admin = new ZKHelixAdmin(zkAddress);

        log.debug("Create clusters");
        admin.addCluster(metaClusterName, true);
        admin.addCluster(managedClusterName, true);

        log.debug("Setup config tool");
        ProviderRebalancerSingleton.setTargetProvider(targetProvider);
        ProviderRebalancerSingleton.setStatusProvider(statusProvider);

        log.debug("Starting target and status provider");
        TestUtils.targetProvider = startAuxService(targetProvider);
        TestUtils.statusProvider = startAuxService(statusProvider);

        // Managed Cluster
        log.debug("Setup managed cluster");
        admin.addStateModelDef(managedClusterName, "MasterSlave", new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave()));
        admin.addResource(managedClusterName, managedResourceName, numManagedPartitions, "MasterSlave", RebalanceMode.FULL_AUTO.toString());
        IdealState managedIdealState = admin.getResourceIdealState(managedClusterName, managedResourceName);
        managedIdealState.setInstanceGroupTag(metaResourceName);
        managedIdealState.setReplicas(String.valueOf(numManagedReplica));
        admin.setResourceIdealState(managedClusterName, managedResourceName, managedIdealState);

        // Meta Cluster
        log.debug("Setup meta cluster");
        admin.addStateModelDef(metaClusterName, "OnlineOffline", new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));
        admin.addResource(metaClusterName, metaResourceName, targetProvider.getTargetContainerCount(metaResourceName), "OnlineOffline",
                RebalanceMode.USER_DEFINED.toString());

        IdealState idealState = admin.getResourceIdealState(metaClusterName, metaResourceName);
        idealState.setRebalancerClassName(ProviderRebalancer.class.getName());
        idealState.setReplicas("1");

        // BEGIN workaround
        // FIXME workaround for HELIX-226
        Map<String, List<String>> listFields = Maps.newHashMap();
        Map<String, Map<String, String>> mapFields = Maps.newHashMap();
        for (int i = 0; i < 256; i++) {
            String partitionName = metaResourceName + "_" + i;
            listFields.put(partitionName, new ArrayList<String>());
            mapFields.put(partitionName, new HashMap<String, String>());
        }
        idealState.getRecord().setListFields(listFields);
        idealState.getRecord().setMapFields(mapFields);
        // END workaround

        admin.setResourceIdealState(metaClusterName, metaResourceName, idealState);

        log.debug("Starting container providers");
        for (Service service : containerProviderProcesses) {
            startProviderService(service);
        }

        log.debug("Starting managed cluster controller");
        managedControllerManager = HelixControllerMain.startHelixController(zkAddress, managedClusterName, "managedController", HelixControllerMain.STANDALONE);

        log.debug("Starting meta cluster controller");
        metaControllerManager = HelixControllerMain.startHelixController(zkAddress, metaClusterName, "metaController", HelixControllerMain.STANDALONE);

        log.debug("Waiting for stable state");
        waitUntilRebalancedCount(targetProvider.getTargetContainerCount(metaResourceName));
    }

    public static void stopTestCluster() throws Exception {
        log.debug(String.format("Stopping test cluster"));
        if (managedControllerManager != null) {
            log.info("Disconnecting managed cluster controller");
            managedControllerManager.disconnect();
        }
        if (metaControllerManager != null) {
            log.info("Disconnecting meta cluster controller");
            metaControllerManager.disconnect();
        }
        log.info("Stopping provider services");
        if (providerServices != null) {
            for (Service service : providerServices) {
                service.stop();
            }
            providerServices.clear();
        }
        log.debug("Stopping auxillary services");
        if (auxServices != null) {
            for (Service service : auxServices) {
                service.stop();
            }
            auxServices.clear();
        }

        admin = null;
        metaControllerManager = null;
        managedControllerManager = null;
    }

    public static <T extends Service> T startAuxService(T service) throws Exception {
        auxServices.add(service);
        service.start();
        return service;
    }

    public static <T extends Service> T startProviderService(T service) throws Exception {
        providerServices.add(service);
        service.start();
        return service;
    }

    public static void rebalanceTestCluster() throws Exception {
        log.debug(String.format("Triggering rebalance"));
        IdealState poke = admin.getResourceIdealState(metaClusterName, metaResourceName);
        admin.setResourceIdealState(metaClusterName, metaResourceName, poke);

        int current = targetProvider.getTargetContainerCount(TestUtils.metaResourceName);
        waitUntilRebalancedCount(current);
    }

    public static void waitUntilRebalancedCount(int containerCount) throws Exception {
        log.debug(String.format("Waiting for rebalance with %d containers at '%s'", containerCount, zkAddress));

        HelixAdmin admin = new ZKHelixAdmin(zkAddress);

        try {
            long limit = System.currentTimeMillis() + REBALANCE_TIMEOUT;
            waitUntilPartitionCount(admin, metaClusterName, metaResourceName, containerCount, (limit - System.currentTimeMillis()));
            waitUntilInstanceCount(admin, metaClusterName, metaResourceName, providerServices.size(), (limit - System.currentTimeMillis()));
            waitUntilPartitionCount(admin, managedClusterName, managedResourceName, numManagedPartitions, (limit - System.currentTimeMillis()));
            
            // FIXME workaround for Helix FULL_AUTO rebalancer not providing guarantees for cluster expansion
            //waitUntilInstanceCount(admin, managedClusterName, managedResourceName, containerCount, (limit - System.currentTimeMillis()));
        } catch (Exception e) {
            throw e;
        } finally {
            admin.close();
        }
    }

    public static void waitUntilInstanceCount(HelixAdmin admin, String cluster, String resource, int targetCount, long timeout) throws Exception {
        log.debug(String.format("Waiting for instance count (cluster='%s', resource='%s', instanceCount=%d, timeout=%d)", cluster, resource, targetCount,
                timeout));

        long limit = System.currentTimeMillis() + timeout;
        while (limit > System.currentTimeMillis()) {
            int assignedCount = getAssingedInstances(admin, cluster, resource).size();
            log.debug(String.format("checking instance count for '%s:%s': target=%d, current=%d", cluster, resource, targetCount, assignedCount));

            if (targetCount == assignedCount) {
                return;
            }
            Thread.sleep(POLL_INTERVAL);
        }
        throw new TimeoutException();
    }

    public static void waitUntilPartitionCount(HelixAdmin admin, String cluster, String resource, int targetCount, long timeout) throws Exception {
        log.debug(String.format("Waiting for partition count (cluster='%s', resource='%s', partitionCount=%d, timeout=%d)", cluster, resource, targetCount,
                timeout));

        long limit = System.currentTimeMillis() + timeout;
        while (limit > System.currentTimeMillis()) {
            int assignedCount = getAssingedPartitions(admin, cluster, resource).size();
            log.debug(String.format("checking partition count for '%s:%s': target=%d, current=%d", cluster, resource, targetCount, assignedCount));

            if (targetCount == assignedCount) {
                return;
            }
            Thread.sleep(POLL_INTERVAL);
        }
        throw new TimeoutException();
    }

    public static Set<String> getAssingedInstances(HelixAdmin admin, String clusterName, String resourceName) {
        Set<String> assignedInstances = new HashSet<String>();

        ExternalView externalView = admin.getResourceExternalView(clusterName, resourceName);

        if (externalView == null)
            return assignedInstances;

        for (String partitionName : externalView.getPartitionSet()) {
            Map<String, String> stateMap = externalView.getStateMap(partitionName);
            if (stateMap == null)
                continue;

            for (String instanceName : stateMap.keySet()) {
                String state = stateMap.get(instanceName);
                if ("MASTER".equals(state) || "SLAVE".equals(state) || "ONLINE".equals(state)) {
                    assignedInstances.add(instanceName);
                }
            }
        }

        return assignedInstances;
    }

    public static Set<String> getAssingedPartitions(HelixAdmin admin, String clusterName, String resourceName) {
        Set<String> assignedPartitions = new HashSet<String>();

        ExternalView externalView = admin.getResourceExternalView(clusterName, resourceName);

        if (externalView == null)
            return assignedPartitions;

        for (String partitionName : externalView.getPartitionSet()) {
            Map<String, String> stateMap = externalView.getStateMap(partitionName);
            if (stateMap == null)
                continue;

            for (String instanceName : stateMap.keySet()) {
                String state = stateMap.get(instanceName);
                if ("MASTER".equals(state) || "ONLINE".equals(state)) {
                    assignedPartitions.add(partitionName);
                }
            }
        }

        return assignedPartitions;
    }

    public static ZkServer createLocalZookeeper() throws Exception {
        String baseDir = "/tmp/autoscale/";
        final String dataDir = baseDir + "zk/dataDir";
        final String logDir = baseDir + "zk/logDir";
        FileUtils.deleteDirectory(new File(dataDir));
        FileUtils.deleteDirectory(new File(logDir));

        IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
            @Override
            public void createDefaultNameSpace(ZkClient zkClient) {

            }
        };
        return new ZkServer(dataDir, logDir, defaultNameSpace, zkPort);
    }

    public static LocalContainerProviderProcess makeLocalProvider(String name) throws Exception {
        LocalContainerProviderProcess process = new LocalContainerProviderProcess();
        process.configure(makeProviderProperties(name));
        return process;
    }

    public static ShellContainerProviderProcess makeShellProvider(String name) throws Exception {
        ShellContainerProviderProcess process = new ShellContainerProviderProcess();
        process.configure(makeProviderProperties(name));
        return process;
    }

    public static YarnContainerProviderProcess makeYarnProvider(String name) throws Exception {
        YarnContainerProviderProperties properties = new YarnContainerProviderProperties();

        properties.putAll(makeProviderProperties(name));
        properties.put(YarnContainerProviderProperties.YARNDATA, zkAddress);
        properties.put(YarnContainerProviderProperties.RESOURCEMANAGER, resmanAddress);
        properties.put(YarnContainerProviderProperties.SCHEDULER, schedulerAddress);
        properties.put(YarnContainerProviderProperties.USER, yarnUser);
        properties.put(YarnContainerProviderProperties.HDFS, hdfsAddress);

        YarnContainerProviderProcess process = new YarnContainerProviderProcess();
        process.configure(properties);

        return process;
    }

    static ProviderProperties makeProviderProperties(String name) {
        ProviderProperties properties = new ProviderProperties();
        properties.putAll(providerProperties);
        properties.setProperty(ProviderProperties.NAME, name);
        return properties;
    }

}
