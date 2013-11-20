package org.apache.helix.autoscale.impl.yarn;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.autoscale.Service;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Implements YARN application master. Continuously monitors container health in
 * YARN and yarn meta data updates. Spawns and destroys containers.
 * 
 */
class YarnMasterService implements Service {

	static final Logger log = Logger.getLogger(YarnMasterService.class);

	static final String REQUIRED_TYPE = "container";

	static final long ZOOKEEPER_TIMEOUT = 5000;
	static final long MASTERSERVICE_INTERVAL = 1000;

	static final String CONTAINERS = "CONTAINERS";

	static final String YARN_CONTAINER_COMMAND = "/bin/sh %s 1>%s/stdout 2>%s/stderr";

	YarnMasterProperties properties;
	AMRMClient<ContainerRequest> protocol;
	ApplicationAttemptId appAttemptId;
	Configuration yarnConfig;
	YarnDataProvider yarnDataService;

	final Map<ContainerId, Container> unassignedContainers = new HashMap<ContainerId, Container>();
	final Map<ContainerId, Container> activeContainers = new HashMap<ContainerId, Container>();
	final Map<ContainerId, ContainerStatus> completedContainers = new HashMap<ContainerId, ContainerStatus>();
	final Map<ContainerId, String> yarn2meta = new HashMap<ContainerId, String>();

	// Counter for completed containers ( complete denotes successful or failed
	// )
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	// Allocated container count so that we know how many containers has the RM
	// allocated to us
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	// Count of failed containers
	private AtomicInteger numFailedContainers = new AtomicInteger();
	// Count of containers already requested from the RM
	// Needed as once requested, we should not request for containers again.
	// Only request for more if the original requirement changes.
	private AtomicInteger numRequestedContainers = new AtomicInteger();

	ScheduledExecutorService executor;

	private AMRMClientAsync<ContainerRequest> amRMClient;

	private AMRMClientAsync.CallbackHandler allocListener;

	private NMCallbackHandler containerListener;

	private NMClientAsyncImpl nmClientAsync;

	public YarnMasterService() {
		Map<String, String> envs = System.getenv();

		ContainerId containerId = ConverterUtils.toContainerId(envs
				.get(Environment.CONTAINER_ID.name()));
		appAttemptId = containerId.getApplicationAttemptId();
		log.info(String.format("Got application attempt id '%s'",
				appAttemptId.toString()));

		log.debug("Connecting to resource manager");
		Configuration conf = new YarnConfiguration();
		conf.set(YarnConfiguration.RM_ADDRESS, properties.getResourceManager());
		conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
				properties.getScheduler());
		conf.set(FileSystem.FS_DEFAULT_NAME_KEY, properties.getHdfs());

		allocListener = new RMCallbackHandler();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(conf);

		containerListener = createNMCallbackHandler();
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(conf);

	}

	@Override
	public void configure(Properties properties) throws Exception {
		YarnMasterProperties yarnProperties = YarnUtils
				.createMasterProperties(properties);
		Preconditions.checkArgument(yarnProperties.isValid());
		this.properties = yarnProperties;
	}

	public void setProtocol(AMRMClient<ContainerRequest> protocol) {
		this.protocol = protocol;
	}

	public void setYarnDataProvider(YarnDataProvider yarnDataService) {
		this.yarnDataService = yarnDataService;
	}

	@Override
	public void start() throws Exception {

		Preconditions.checkNotNull(properties);
		Preconditions.checkNotNull(protocol);
		Preconditions.checkNotNull(appAttemptId);
		Preconditions.checkNotNull(yarnConfig);
		Preconditions.checkNotNull(yarnDataService);

		log.debug("starting yarn master service");
		amRMClient.start();
		nmClientAsync.start();

		// register the AM with the RM
		// Register self with ResourceManager
		// This will start heartbeating to the RM
		String appMasterHostname = NetUtils.getHostname();
		int appMasterRpcPort = -1;
		String appMasterTrackingUrl = "";
		log.debug("Registering application master");
		RegisterApplicationMasterResponse response = amRMClient
				.registerApplicationMaster(appMasterHostname, appMasterRpcPort,
						appMasterTrackingUrl);

		// Dump out information about cluster capability as seen by the
		// resource manager
		int maxMem = response.getMaximumResourceCapability().getMemory();
		log.info("Max mem capabililty of resources in this cluster " + maxMem);

		int maxVCores = response.getMaximumResourceCapability()
				.getVirtualCores();
		log.info("Max vcores capabililty of resources in this cluster "
				+ maxVCores);

		executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(new YarnService(), 0,
				MASTERSERVICE_INTERVAL, TimeUnit.MILLISECONDS);
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

	@VisibleForTesting
	NMCallbackHandler createNMCallbackHandler() {
		return new NMCallbackHandler(this);
	}

	Collection<YarnContainerData> readOwnedYarnData() throws Exception {
		log.debug("reading container data");

		Collection<YarnContainerData> containers = new ArrayList<YarnContainerData>();
		for (YarnContainerData meta : yarnDataService.readAll()) {
			if (meta.owner.equals(properties.getName())) {
				containers.add(meta);
				log.debug(String
						.format("found container node '%s' (state=%s, yarnId=%s, owner=%s)",
								meta.id, meta.state, meta.yarnId, meta.owner));
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
				int numYarn = numYarnUnassigned + numYarnActive
						+ numYarnCompleted;

				int numYarnRequired = numMetaAcquire - numYarnUnassigned;

				// additionally required containers
				int numRequestAdditional = Math.max(0, numYarnRequired);

				// overstock containers
				List<ContainerId> unneededReleasedIds = createOverstockReleaseList(numYarnRequired);

				int numReleased = destroyedReleasedIds.size()
						+ unneededReleasedIds.size();

				log.debug(String
						.format("meta containers (total=%d, acquire=%d, active=%d, completed=%d)",
								numMeta, numMetaAcquire, numMetaActive,
								numMetaCompleted));
				log.debug(String
						.format("yarn containers (total=%d, unassigned=%d, active=%d, completed=%d)",
								numYarn, numYarnUnassigned, numYarnActive,
								numYarnCompleted));
				log.debug(String.format(
						"requesting %d new containers, releasing %d",
						numRequestAdditional, numReleased));

				// remove unassigned container about to be freed
				for (ContainerId id : unneededReleasedIds) {
					log.info(String
							.format("Unassigned container '%s' about to be freed, removing",
									id));
					unassignedContainers.remove(id);
					ContainerRequest req;
					amRMClient.releaseAssignedContainer(id);
				}

				Iterator<Container> itYarn = unassignedContainers.values()
						.iterator();
				Iterator<YarnContainerData> itMeta = yarndata.iterator();
				while (itYarn.hasNext() && itMeta.hasNext()) {
					YarnContainerData meta = itMeta.next();

					if (meta.yarnId >= 0)
						continue;

					Container containerYarn = itYarn.next();

					log.debug(String
							.format("assigning yarn container '%s' to container node '%s'",
									containerYarn.getId(), meta.id));

					String command = String.format(YARN_CONTAINER_COMMAND,
							YarnUtils.YARN_CONTAINER_PATH,
							ApplicationConstants.LOG_DIR_EXPANSION_VAR,
							ApplicationConstants.LOG_DIR_EXPANSION_VAR);

					log.debug(String.format("Running container command \"%s\"",
							command));

					// configuration
					YarnContainerProcessProperties containerProp = meta
							.getProperties();
					containerProp.setProperty(
							YarnContainerProcessProperties.ADDRESS,
							properties.getAddress());
					containerProp.setProperty(
							YarnContainerProcessProperties.CLUSTER,
							properties.getCluster());
					containerProp.setProperty(
							YarnContainerProcessProperties.YARNDATA,
							properties.getYarnData());
					containerProp.setProperty(
							YarnContainerProcessProperties.NAME, meta.id);

					File propertiesFile = YarnUtils
							.writePropertiesToTemp(containerProp);

					// HDFS
					final String namespace = appAttemptId.getApplicationId()
							.toString() + "/" + meta.id;
					final Path containerArchive = YarnUtils.copyToHdfs(
							YarnUtils.YARN_CONTAINER_STAGING,
							YarnUtils.YARN_CONTAINER_STAGING, namespace,
							yarnConfig);
					final Path containerProperties = YarnUtils.copyToHdfs(
							propertiesFile.getCanonicalPath(),
							YarnUtils.YARN_CONTAINER_PROPERTIES, namespace,
							yarnConfig);

					// local resources
					Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
					localResources.put(YarnUtils.YARN_CONTAINER_DESTINATION,
							YarnUtils.createHdfsResource(containerArchive,
									LocalResourceType.ARCHIVE, yarnConfig));
					localResources.put(YarnUtils.YARN_CONTAINER_PROPERTIES,
							YarnUtils.createHdfsResource(containerProperties,
									LocalResourceType.FILE, yarnConfig));

					ContainerLaunchContext context = Records
							.newRecord(ContainerLaunchContext.class);
					context.setEnvironment(Maps.<String, String> newHashMap());
					context.setCommands(Collections.singletonList(command));
					context.setLocalResources(localResources);

					log.debug(String.format(
							"container '%s' executing command '%s'", meta.id,
							command));

					containerListener.addContainer(containerYarn.getId(),
							containerYarn);
					nmClientAsync.startContainerAsync(containerYarn, context);

					log.debug(String.format(
							"container '%s' started, updating container node",
							meta.id));

					meta.setProperties(containerProp);
					meta.setState(YarnContainerData.ContainerState.CONNECTING);
					meta.setYarnId(containerYarn.getId().getId());
					yarnDataService.update(meta);

					yarn2meta.put(containerYarn.getId(), meta.id);

					log.debug(String
							.format("removing '%s' from unassigned yarn containers and adding to active list",
									containerYarn.getId()));

					itYarn.remove();
					activeContainers.put(containerYarn.getId(), containerYarn);

					// cleanup
					propertiesFile.deleteOnExit();

				}
				log.debug("yarn service update cycle complete");

			} catch (Exception e) {
				log.error("Error while executing yarn update cycle", e);
			}
		}

		private List<ContainerId> createOverstockReleaseList(int numYarnRequired) {
			List<ContainerId> unneededReleasedIds = new ArrayList<ContainerId>();
			Iterator<Container> itUnassigned = unassignedContainers.values()
					.iterator();
			if (numYarnRequired < 0) {
				for (int i = 0; i < -numYarnRequired && itUnassigned.hasNext(); i++) {
					Container container = itUnassigned.next();
					unneededReleasedIds.add(container.getId());
					log.debug(String.format(
							"Container '%s' no longer required, removing",
							container.getId()));
					itUnassigned.remove();
				}
			}
			return unneededReleasedIds;
		}

		private List<ContainerId> createDestroyedReleaseList(
				Collection<YarnContainerData> yarndata) {
			List<ContainerId> releasedIds = new ArrayList<ContainerId>();
			for (YarnContainerData meta : yarndata) {
				if (meta.state == YarnContainerData.ContainerState.HALTED) {
					ContainerId containerId = ContainerId.newInstance(
							appAttemptId, meta.yarnId);
					releasedIds.add(containerId);
					log.debug(String.format("releasing container '%s'",
							containerId));
				}
			}
			return releasedIds;
		}

		private int countAcquireMeta(Collection<YarnContainerData> yarndata) {
			int numMetaAcquire = 0;
			for (YarnContainerData meta : yarndata) {
				if (meta.state == YarnContainerData.ContainerState.ACQUIRE) {
					numMetaAcquire++;
				}
			}
			return numMetaAcquire;
		}

		private int countActiveMeta(Collection<YarnContainerData> yarndata) {
			int numMetaActive = 0;
			for (YarnContainerData meta : yarndata) {
				if (meta.state != YarnContainerData.ContainerState.ACQUIRE
						&& meta.state != YarnContainerData.ContainerState.HALTED
						&& meta.state != YarnContainerData.ContainerState.FINALIZE) {
					numMetaActive++;
				}
			}
			return numMetaActive;
		}
	}

	public static void destroyLocalMasterNamespace() {
		log.info("cleaning up master directory");
		FileUtils.deleteQuietly(new File(YarnUtils.YARN_MASTER_DESTINATION));
		FileUtils.deleteQuietly(new File(YarnUtils.YARN_MASTER_PROPERTIES));
		FileUtils.deleteQuietly(new File(YarnUtils.YARN_CONTAINER_STAGING));
	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
		@SuppressWarnings("unchecked")
		@Override
		public void onContainersCompleted(
				List<ContainerStatus> completedContainersStatuses) {
			log.info("Got response from RM for container ask, completedCnt="
					+ completedContainers.size());
			for (ContainerStatus containerStatus : completedContainersStatuses) {
				log.info("Got container status for containerID="
						+ containerStatus.getContainerId() + ", state="
						+ containerStatus.getState() + ", exitStatus="
						+ containerStatus.getExitStatus() + ", diagnostics="
						+ containerStatus.getDiagnostics());

				// non complete containers should not be here
				assert (containerStatus.getState() == ContainerState.COMPLETE);

				// increment counters for completed/failed containers
				int exitStatus = containerStatus.getExitStatus();
				if (0 != exitStatus) {
					// container failed
					if (ContainerExitStatus.ABORTED != exitStatus) {
						// shell script failed
						// counts as completed
						numCompletedContainers.incrementAndGet();
						numFailedContainers.incrementAndGet();
					} else {
						// container was killed by framework, possibly preempted
						// we should re-try as the container was lost for some
						// reason
						numAllocatedContainers.decrementAndGet();
						numRequestedContainers.decrementAndGet();
						// we do not need to release the container as it would
						// be done
						// by the RM
					}
				} else {
					// nothing to do
					// container completed successfully
					numCompletedContainers.incrementAndGet();
					log.info("Container completed successfully."
							+ ", containerId="
							+ containerStatus.getContainerId());
				}
			}
			for (ContainerStatus status : completedContainersStatuses) {
				ContainerId id = status.getContainerId();

				log.info(String.format("Container '%s' completed", id));

				if (unassignedContainers.containsKey(id)) {
					log.info(String.format(
							"Unassigned container '%s' terminated, removing",
							id));
					unassignedContainers.remove(id);
				}

				if (activeContainers.containsKey(id)) {
					log.info(String.format(
							"Active container '%s' terminated, removing", id));
					activeContainers.remove(id);

					String metaId = yarn2meta.get(id);
					try {
						YarnContainerData meta = yarnDataService.read(metaId);

						log.debug(String
								.format("container '%s' finalized, updating container node",
										meta.id));

						yarnDataService
								.update(meta
										.setState(YarnContainerData.ContainerState.FINALIZE));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				completedContainers.put(id, status);
			}
		}

		@Override
		public void onContainersAllocated(List<Container> allocatedContainers) {
			log.info("Got response from RM for container ask, allocatedCnt="
					+ allocatedContainers.size());
			numAllocatedContainers.addAndGet(allocatedContainers.size());
			log.info(String.format("%d new containers available",
					allocatedContainers.size()));

			for (Container allocatedContainer : allocatedContainers) {
				log.info("Launching shell command on a new container."
						+ ", containerId=" + allocatedContainer.getId()
						+ ", containerNode="
						+ allocatedContainer.getNodeId().getHost() + ":"
						+ allocatedContainer.getNodeId().getPort()
						+ ", containerNodeURI="
						+ allocatedContainer.getNodeHttpAddress()
						+ ", containerResourceMemory"
						+ allocatedContainer.getResource().getMemory()
						+ ", containerResourceVirtualCores"
						+ allocatedContainer.getResource().getVirtualCores());
				// + ", containerToken"
				// +allocatedContainer.getContainerToken().getIdentifier().toString());
				unassignedContainers.put(allocatedContainer.getId(),
						allocatedContainer);

			}
		}

		@Override
		public void onShutdownRequest() {
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {
		}

		@Override
		public float getProgress() {
			// set progress to deliver to RM on next heartbeat
			return 0.5f;
		}

		@Override
		public void onError(Throwable e) {
			amRMClient.stop();
		}
	}

	@VisibleForTesting
	static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
		private final YarnMasterService applicationMaster;

		public NMCallbackHandler(YarnMasterService applicationMaster) {
			this.applicationMaster = applicationMaster;
		}

		public void addContainer(ContainerId containerId, Container container) {
			containers.putIfAbsent(containerId, container);
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			if (log.isDebugEnabled()) {
				log.debug("Succeeded to stop Container " + containerId);
			}
			containers.remove(containerId);

		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId,
				ContainerStatus containerStatus) {
			if (log.isDebugEnabled()) {
				log.debug("Container Status: id=" + containerId + ", status="
						+ containerStatus);
			}
		}

		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
			if (log.isDebugEnabled()) {
				log.debug("Succeeded to start Container " + containerId);
			}
			Container container = containers.get(containerId);
			if (container != null) {
				applicationMaster.nmClientAsync.getContainerStatusAsync(
						containerId, container.getNodeId());
			}
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			log.error("Failed to start Container " + containerId);
			containers.remove(containerId);
			applicationMaster.numCompletedContainers.incrementAndGet();
			applicationMaster.numFailedContainers.incrementAndGet();
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId,
				Throwable t) {
			log.error("Failed to query the status of Container " + containerId);
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			log.error("Failed to stop Container " + containerId);
			containers.remove(containerId);
		}
	}

	/**
	 * Setup the request that will be sent to the RM for the container ask.
	 * 
	 * @return the setup ResourceRequest to be sent to RM
	 */
	private ContainerRequest setupContainerAskForRM() {
		// setup requirements for hosts
		// using * as any host will do for the distributed shell app
		// set the priority for the request
		Priority pri = Records.newRecord(Priority.class);
		int requestPriority = 1;
		// TODO - what is the range for priority? how to decide?
		pri.setPriority(requestPriority);

		// Set up resource type requirements
		// For now, memory and CPU are supported so we set memory and cpu
		// requirements
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(512);
		capability.setVirtualCores(1);

		ContainerRequest request = new ContainerRequest(capability, null, null,
				pri);
		log.info("Requested container ask: " + request.toString());
		return request;
	}
}
