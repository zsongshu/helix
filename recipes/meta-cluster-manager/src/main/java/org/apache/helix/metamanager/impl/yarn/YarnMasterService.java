package org.apache.helix.metamanager.impl.yarn;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.metamanager.Service;
import org.apache.helix.metamanager.impl.yarn.YarnContainerData.ContainerState;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Implements YARN application master. Continuously monitors container health in
 * YARN and yarn meta data updates. Spawns and destroys containers.
 * 
 */
class YarnMasterService implements Service {

    static final Logger                     log                    = Logger.getLogger(YarnMasterService.class);

    static final String                     REQUIRED_TYPE          = "container";

    static final long                       ZOOKEEPER_TIMEOUT      = 5000;
    static final long                       MASTERSERVICE_INTERVAL = 1000;

    static final String                     CONTAINERS             = "CONTAINERS";

    static final String                     YARN_CONTAINER_COMMAND = "/bin/sh %s 1>%s/stdout 2>%s/stderr";

    YarnMasterProperties                    properties;
    AMRMProtocol                            protocol;
    ApplicationAttemptId                    attemptId;
    Configuration                           yarnConfig;
    YarnDataProvider                        yarnDataService;

    final Map<ContainerId, Container>       unassignedContainers   = new HashMap<ContainerId, Container>();
    final Map<ContainerId, Container>       activeContainers       = new HashMap<ContainerId, Container>();
    final Map<ContainerId, ContainerStatus> completedContainers    = new HashMap<ContainerId, ContainerStatus>();
    final Map<ContainerId, String>          yarn2meta              = new HashMap<ContainerId, String>();

    ScheduledExecutorService                executor;

    @Override
    public void configure(Properties properties) throws Exception {
        YarnMasterProperties yarnProperties = YarnUtils.createMasterProperties(properties);
        Preconditions.checkArgument(yarnProperties.isValid());
        this.properties = yarnProperties;
    }

    public void setProtocol(AMRMProtocol protocol) {
        this.protocol = protocol;
    }

    public void setAttemptId(ApplicationAttemptId attemptId) {
        this.attemptId = attemptId;
    }

    public void setYarnConfiguration(Configuration yarnConfig) {
        this.yarnConfig = yarnConfig;
    }

    public void setYarnDataProvider(YarnDataProvider yarnDataService) {
        this.yarnDataService = yarnDataService;
    }

    @Override
    public void start() {
        Preconditions.checkNotNull(properties);
        Preconditions.checkNotNull(protocol);
        Preconditions.checkNotNull(attemptId);
        Preconditions.checkNotNull(yarnConfig);
        Preconditions.checkNotNull(yarnDataService);

        log.debug("starting yarn master service");

        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new YarnService(), 0, MASTERSERVICE_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        log.debug("stopping yarn master service");

        if (executor != null) {
            executor.shutdown();
            while (!executor.isTerminated()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            executor = null;
        }

        destroyLocalMasterNamespace();
    }

    Collection<YarnContainerData> readOwnedYarnData() throws Exception {
        log.debug("reading container data");

        Collection<YarnContainerData> containers = new ArrayList<YarnContainerData>();
        for (YarnContainerData meta : yarnDataService.readAll()) {
            if (meta.owner.equals(properties.getName())) {
                containers.add(meta);
                log.debug(String.format("found container node '%s' (state=%s, yarnId=%s, owner=%s)", meta.id, meta.state, meta.yarnId, meta.owner));
            }
        }
        return containers;
    }

    class YarnService implements Runnable {
        int responseId = 0;

        @Override
        public void run() {
            try {
                log.debug("running yarn service update cycle");

                Collection<YarnContainerData> yarndata = readOwnedYarnData();

                // active meta containers
                int numMetaActive = countActiveMeta(yarndata);

                // newly acquired meta containers
                int numMetaAcquire = countAcquireMeta(yarndata);

                // destroyed meta containers
                List<ContainerId> destroyedReleasedIds = createDestroyedReleaseList(yarndata);
                int numMetaCompleted = destroyedReleasedIds.size();

                int numMeta = numMetaAcquire + numMetaActive + numMetaCompleted;

                // yarn containers
                int numYarnUnassigned = unassignedContainers.size();
                int numYarnActive = activeContainers.size();
                int numYarnCompleted = completedContainers.size();
                int numYarn = numYarnUnassigned + numYarnActive + numYarnCompleted;

                int numYarnRequired = numMetaAcquire - numYarnUnassigned;

                // additionally required containers
                int numRequestAdditional = Math.max(0, numYarnRequired);

                // overstock containers
                List<ContainerId> unneededReleasedIds = createOverstockReleaseList(numYarnRequired);

                int numReleased = destroyedReleasedIds.size() + unneededReleasedIds.size();

                log.debug(String.format("meta containers (total=%d, acquire=%d, active=%d, completed=%d)", numMeta, numMetaAcquire, numMetaActive, numMetaCompleted));
                log.debug(String.format("yarn containers (total=%d, unassigned=%d, active=%d, completed=%d)", numYarn, numYarnUnassigned, numYarnActive, numYarnCompleted));
                log.debug(String.format("requesting %d new containers, releasing %d", numRequestAdditional, numReleased));

                Priority priority = Records.newRecord(Priority.class);
                priority.setPriority(0);

                Resource resource = Records.newRecord(Resource.class);
                resource.setMemory(256); // TODO make dynamic

                ResourceRequest resourceRequest = Records.newRecord(ResourceRequest.class);
                resourceRequest.setHostName("*");
                resourceRequest.setNumContainers(numRequestAdditional);
                resourceRequest.setPriority(priority);
                resourceRequest.setCapability(resource);

                AllocateRequest request = Records.newRecord(AllocateRequest.class);
                request.setResponseId(responseId);
                request.setApplicationAttemptId(attemptId);
                request.addAsk(resourceRequest);
                request.addAllReleases(destroyedReleasedIds);
                request.addAllReleases(unneededReleasedIds);

                responseId++;

                AllocateResponse allocateResponse = null;
                try {
                    allocateResponse = protocol.allocate(request);
                } catch (YarnRemoteException e) {
                    // ignore
                    log.error("Error allocating containers", e);
                    return;
                }

                AMResponse response = allocateResponse.getAMResponse();

                // remove unassigned container about to be freed
                for (ContainerId id : unneededReleasedIds) {
                    log.info(String.format("Unassigned container '%s' about to be freed, removing", id));
                    unassignedContainers.remove(id);
                }

                // newly added containers
                for (Container container : response.getAllocatedContainers()) {
                    unassignedContainers.put(container.getId(), container);
                }

                log.info(String.format("%d new containers available, %d required", unassignedContainers.size(), numMetaAcquire));

                Iterator<Container> itYarn = unassignedContainers.values().iterator();
                Iterator<YarnContainerData> itMeta = yarndata.iterator();
                while (itYarn.hasNext() && itMeta.hasNext()) {
                    YarnContainerData meta = itMeta.next();

                    if (meta.yarnId >= 0)
                        continue;

                    Container containerYarn = itYarn.next();

                    log.debug(String.format("assigning yarn container '%s' to container node '%s'", containerYarn.getId(), meta.id));

                    String command = String.format(YARN_CONTAINER_COMMAND, YarnUtils.YARN_CONTAINER_PATH, ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                            ApplicationConstants.LOG_DIR_EXPANSION_VAR);

                    log.debug(String.format("Running container command \"%s\"", command));

                    // configuration
                    YarnContainerProcessProperties containerProp = meta.getProperties();
                    containerProp.setProperty(YarnContainerProcessProperties.ADDRESS, properties.getAddress());
                    containerProp.setProperty(YarnContainerProcessProperties.CLUSTER, properties.getCluster());
                    containerProp.setProperty(YarnContainerProcessProperties.YARNDATA, properties.getYarnData());
                    containerProp.setProperty(YarnContainerProcessProperties.NAME, meta.id);

                    File propertiesFile = YarnUtils.writePropertiesToTemp(containerProp);

                    // HDFS
                    final String namespace = attemptId.getApplicationId().toString() + "/" + meta.id;
                    final Path containerArchive = YarnUtils.copyToHdfs(YarnUtils.YARN_CONTAINER_STAGING, YarnUtils.YARN_CONTAINER_STAGING, namespace, yarnConfig);
                    final Path containerProperties = YarnUtils.copyToHdfs(propertiesFile.getCanonicalPath(), YarnUtils.YARN_CONTAINER_PROPERTIES, namespace, yarnConfig);

                    // local resources
                    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
                    localResources.put(YarnUtils.YARN_CONTAINER_DESTINATION,
                            YarnUtils.createHdfsResource(containerArchive, LocalResourceType.ARCHIVE, yarnConfig));
                    localResources.put(YarnUtils.YARN_CONTAINER_PROPERTIES,
                            YarnUtils.createHdfsResource(containerProperties, LocalResourceType.FILE, yarnConfig));

                    ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
                    context.setContainerId(containerYarn.getId());
                    context.setResource(containerYarn.getResource());
                    context.setEnvironment(Maps.<String, String> newHashMap());
                    context.setCommands(Collections.singletonList(command));
                    context.setLocalResources(localResources);
                    context.setUser(properties.getUser());

                    log.debug(String.format("container '%s' executing command '%s'", meta.id, command));

                    StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
                    startReq.setContainerLaunchContext(context);

                    try {
                        getContainerManager(containerYarn).startContainer(startReq);

                    } catch (YarnRemoteException e) {
                        log.error(String.format("Error starting container '%s'", meta.id), e);
                        return;
                    }

                    log.debug(String.format("container '%s' started, updating container node", meta.id));

                    meta.setProperties(containerProp);
                    meta.setState(ContainerState.CONNECTING);
                    meta.setYarnId(containerYarn.getId().getId());
                    yarnDataService.update(meta);

                    yarn2meta.put(containerYarn.getId(), meta.id);

                    log.debug(String.format("removing '%s' from unassigned yarn containers and adding to active list", containerYarn.getId()));

                    itYarn.remove();
                    activeContainers.put(containerYarn.getId(), containerYarn);

                    // cleanup
                    propertiesFile.deleteOnExit();

                }

                for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                    ContainerId id = status.getContainerId();

                    log.info(String.format("Container '%s' completed", id));

                    if (unassignedContainers.containsKey(id)) {
                        log.info(String.format("Unassigned container '%s' terminated, removing", id));
                        unassignedContainers.remove(id);
                    }

                    if (activeContainers.containsKey(id)) {
                        log.info(String.format("Active container '%s' terminated, removing", id));
                        activeContainers.remove(id);

                        String metaId = yarn2meta.get(id);
                        YarnContainerData meta = yarnDataService.read(metaId);

                        log.debug(String.format("container '%s' finalized, updating container node", meta.id));

                        yarnDataService.update(meta.setState(ContainerState.FINALIZE));
                    }

                    completedContainers.put(id, status);
                }

                log.debug("yarn service update cycle complete");

            } catch (Exception e) {
                log.error("Error while executing yarn update cycle", e);
            }
        }

        private List<ContainerId> createOverstockReleaseList(int numYarnRequired) {
            List<ContainerId> unneededReleasedIds = new ArrayList<ContainerId>();
            Iterator<Container> itUnassigned = unassignedContainers.values().iterator();
            if (numYarnRequired < 0) {
                for (int i = 0; i < -numYarnRequired && itUnassigned.hasNext(); i++) {
                    Container container = itUnassigned.next();
                    unneededReleasedIds.add(container.getId());
                    log.debug(String.format("Container '%s' no longer required, removing", container.getId()));
                    itUnassigned.remove();
                }
            }
            return unneededReleasedIds;
        }

        private List<ContainerId> createDestroyedReleaseList(Collection<YarnContainerData> yarndata) {
            List<ContainerId> releasedIds = new ArrayList<ContainerId>();
            for (YarnContainerData meta : yarndata) {
                if (meta.state == ContainerState.HALTED) {
                    ContainerId containerId = Records.newRecord(ContainerId.class);
                    containerId.setApplicationAttemptId(attemptId);
                    containerId.setId(meta.yarnId);
                    releasedIds.add(containerId);
                    log.debug(String.format("releasing container '%s'", containerId));
                }
            }
            return releasedIds;
        }

        private int countAcquireMeta(Collection<YarnContainerData> yarndata) {
            int numMetaAcquire = 0;
            for (YarnContainerData meta : yarndata) {
                if (meta.state == ContainerState.ACQUIRE) {
                    numMetaAcquire++;
                }
            }
            return numMetaAcquire;
        }

        private int countActiveMeta(Collection<YarnContainerData> yarndata) {
            int numMetaActive = 0;
            for (YarnContainerData meta : yarndata) {
                if (meta.state != ContainerState.ACQUIRE && meta.state != ContainerState.HALTED && meta.state != ContainerState.FINALIZE) {
                    numMetaActive++;
                }
            }
            return numMetaActive;
        }
    }

    private ContainerManager getContainerManager(Container container) {
        YarnConfiguration yarnConf = new YarnConfiguration(yarnConfig);
        YarnRPC rpc = YarnRPC.create(yarnConf);
        NodeId nodeId = container.getNodeId();
        String containerIpPort = String.format("%s:%d", nodeId.getHost(), nodeId.getPort());
        log.info("Connecting to ContainerManager at: " + containerIpPort);
        InetSocketAddress addr = NetUtils.createSocketAddr(containerIpPort);
        ContainerManager cm = (ContainerManager) rpc.getProxy(ContainerManager.class, addr, yarnConfig);
        return cm;
    }

    public static void destroyLocalMasterNamespace() {
        log.info("cleaning up master directory");
        FileUtils.deleteQuietly(new File(YarnUtils.YARN_MASTER_DESTINATION));
        FileUtils.deleteQuietly(new File(YarnUtils.YARN_MASTER_PROPERTIES));
        FileUtils.deleteQuietly(new File(YarnUtils.YARN_CONTAINER_STAGING));
    }

}
