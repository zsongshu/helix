package org.apache.helix.provisioning.mesos;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.config.ContainerConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerProvider;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.helix.controller.provisioner.Provisioner;
import org.apache.helix.controller.provisioner.TargetProvider;
import org.apache.helix.controller.provisioner.TargetProviderResponse;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

public class MesosProvisioner implements Provisioner, TargetProvider, ContainerProvider {
  private static final Logger LOG = Logger.getLogger(MesosProvisioner.class);
  private HelixManager _manager;
  private ResourceConfig _resourceConfig;
  private MesosProvisionerConfig _config;
  private ProvisionerScheduler _scheduler;
  private List<ListenableFuture<ContainerId>> _allocFutures = Lists.newLinkedList();
  private List<ListenableFuture<Boolean>> _deallocFutures = Lists.newLinkedList();
  private List<ListenableFuture<Boolean>> _startFutures = Lists.newLinkedList();
  private List<ListenableFuture<Boolean>> _stopFutures = Lists.newLinkedList();

  @Override
  public ListenableFuture<ContainerId> allocateContainer(ContainerSpec spec) {
    SettableFuture<ContainerId> future = SettableFuture.create();
    _allocFutures.add(future);
    return future;
  }

  @Override
  public ListenableFuture<Boolean> deallocateContainer(ContainerId containerId) {
    SettableFuture<Boolean> future = SettableFuture.create();
    _deallocFutures.add(future);
    return future;
  }

  @Override
  public ListenableFuture<Boolean> startContainer(ContainerId containerId, Participant participant) {
    SettableFuture<Boolean> future = SettableFuture.create();
    _startFutures.add(future);
    return future;
  }

  @Override
  public ListenableFuture<Boolean> stopContainer(ContainerId containerId) {
    SettableFuture<Boolean> future = SettableFuture.create();
    _stopFutures.add(future);
    return future;
  }

  @Override
  public TargetProviderResponse evaluateExistingContainers(Cluster cluster, ResourceId resourceId,
      Collection<Participant> participants) {
    TargetProviderResponse response = new TargetProviderResponse();
    // ask for two containers at a time
    List<ContainerSpec> containersToAcquire = Lists.newArrayList();
    List<Participant> containersToStart = Lists.newArrayList();
    List<Participant> containersToRelease = Lists.newArrayList();
    List<Participant> containersToStop = Lists.newArrayList();
    int targetNumContainers = _config.getNumContainers();

    // Any container that is in a state should be put in this set
    Set<ParticipantId> existingContainersIdSet = new HashSet<ParticipantId>();

    // Cache halted containers to determine which to restart and which to release
    Map<ParticipantId, Participant> excessHaltedContainers = Maps.newHashMap();

    // Cache participants to ensure that excess participants are stopped
    Map<ParticipantId, Participant> excessActiveContainers = Maps.newHashMap();

    for (Participant participant : participants) {
      ContainerConfig containerConfig = participant.getContainerConfig();
      if (containerConfig != null && containerConfig.getState() != null) {
        ContainerState state = containerConfig.getState();
        switch (state) {
        case ACQUIRING:
          existingContainersIdSet.add(participant.getId());
          break;
        case ACQUIRED:
          // acquired containers are ready to start
          existingContainersIdSet.add(participant.getId());
          containersToStart.add(participant);
          break;
        case CONNECTING:
          existingContainersIdSet.add(participant.getId());
          break;
        case CONNECTED:
          // active containers can be stopped or kept active
          existingContainersIdSet.add(participant.getId());
          excessActiveContainers.put(participant.getId(), participant);
          break;
        case DISCONNECTED:
          // disconnected containers must be stopped
          existingContainersIdSet.add(participant.getId());
          containersToStop.add(participant);
        case HALTING:
          existingContainersIdSet.add(participant.getId());
          break;
        case HALTED:
          // halted containers can be released or restarted
          existingContainersIdSet.add(participant.getId());
          excessHaltedContainers.put(participant.getId(), participant);
          break;
        case FINALIZING:
          existingContainersIdSet.add(participant.getId());
          break;
        case FINALIZED:
          break;
        case FAILED:
          // remove the failed instance
          _manager.getClusterManagmentTool().dropInstance(cluster.getId().toString(),
              new InstanceConfig(participant.getId()));
          break;
        default:
          break;
        }
      }
    }

    for (int i = 0; i < targetNumContainers; i++) {
      ParticipantId participantId = ParticipantId.from(resourceId + "_container_" + (i));
      excessActiveContainers.remove(participantId); // don't stop this container if active
      if (excessHaltedContainers.containsKey(participantId)) {
        // Halted containers can be restarted if necessary
        // Participant participant = excessHaltedContainers.get(participantId);
        // containersToStart.add(participant);
        // excessHaltedContainers.remove(participantId); // don't release this container
      } else if (!existingContainersIdSet.contains(participantId)) {
        // Unallocated containers must be allocated
        ContainerSpec containerSpec = new ContainerSpec(participantId);
        containerSpec.setMemory(_resourceConfig.getUserConfig().getIntField("memory", 1024));
        containersToAcquire.add(containerSpec);
      }
    }

    // Add all the containers that should be stopped because they fall outside the target range
    containersToStop.addAll(excessActiveContainers.values());

    // Add halted containers that should not be restarted
    containersToRelease.addAll(excessHaltedContainers.values());

    response.setContainersToAcquire(containersToAcquire);
    response.setContainersToStart(containersToStart);
    response.setContainersToRelease(containersToRelease);
    response.setContainersToStop(containersToStop);
    LOG.info("target provider response containers to acquire:" + response.getContainersToAcquire());
    LOG.info("target provider response containers to start:" + response.getContainersToStart());
    return response;
  }

  @Override
  public void init(HelixManager helixManager, ResourceConfig resourceConfig) {
    _manager = helixManager;
    _resourceConfig = resourceConfig;
    _config = (MesosProvisionerConfig) resourceConfig.getProvisionerConfig();
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          initInternal(_config);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    t.setDaemon(true);
    t.start();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public ContainerProvider getContainerProvider() {
    return this;
  }

  @Override
  public TargetProvider getTargetProvider() {
    return this;
  }

  private void initInternal(MesosProvisionerConfig config) throws IOException {
    String uri =
        new File(
            "/Users/kbiscuit/helix/incubator-helix/helix-provisioning-mesos/target/helix-provisioning-mesos-pkg/bin/test-executor.sh")
            .getCanonicalPath();

    ExecutorInfo executor =
        ExecutorInfo.newBuilder().setExecutorId(ExecutorID.newBuilder().setValue("default"))
            .setCommand(CommandInfo.newBuilder().setValue(uri)).setName("Helix Executor (Java)")
            .setSource("java_test").build();

    FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder().setUser("") // Have Mesos
                                                                                    // fill in the
                                                                                    // current user.
        .setName("Test Framework (Java)");

    if (System.getenv("MESOS_CHECKPOINT") != null) {
      System.out.println("Enabling checkpoint for the framework");
      frameworkBuilder.setCheckpoint(true);
    }

    FrameworkInfo framework = frameworkBuilder.build();

    _scheduler = new ProvisionerScheduler(executor);

    MesosSchedulerDriver driver = null;
    if (System.getenv("MESOS_AUTHENTICATE") != null) {
      System.out.println("Enabling authentication for the framework");

      if (System.getenv("DEFAULT_PRINCIPAL") == null) {
        System.err.println("Expecting authentication principal in the environment");
        System.exit(1);
      }

      if (System.getenv("DEFAULT_SECRET") == null) {
        System.err.println("Expecting authentication secret in the environment");
        System.exit(1);
      }

      Credential credential =
          Credential.newBuilder().setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
              .setSecret(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes())).build();

      driver =
          new MesosSchedulerDriver(_scheduler, framework, config.getMasterAddress(), credential);
    } else {
      driver = new MesosSchedulerDriver(_scheduler, framework, config.getMasterAddress());
    }

    driver.run();
  }

}
