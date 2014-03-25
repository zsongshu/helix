package org.apache.helix.provisioning.mesos;

import java.util.List;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.google.common.collect.Lists;

public class ProvisionerScheduler implements Scheduler {

  private int launchedTasks = 0;
  private ExecutorInfo executor;

  public ProvisionerScheduler(ExecutorInfo executor) {
    this.executor = executor;
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    System.err.println("registered");
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    System.err.println("reregistered");

  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    System.err.println("offers");
    List<OfferID> offerIds = Lists.newArrayList();
    List<TaskInfo> tasks = Lists.newArrayList();
    for (Offer offer : offers) {
      TaskID taskId = TaskID.newBuilder().setValue(Integer.toString(launchedTasks++)).build();

      System.out.println("Launching task " + taskId.getValue());

      TaskInfo task =
          TaskInfo
              .newBuilder()
              .setName("task " + taskId.getValue())
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId())
              .addResources(
                  Resource.newBuilder().setName("cpus").setType(Type.SCALAR)
                      .setScalar(Scalar.newBuilder().setValue(1)))
              .addResources(
                  Resource.newBuilder().setName("mem").setType(Type.SCALAR)
                      .setScalar(Scalar.newBuilder().setValue(128)))
              .setExecutor(ExecutorInfo.newBuilder(executor)).build();
      tasks.add(task);
      offerIds.add(offer.getId());
      break;
    }
    // Filters filters = Filters.newBuilder().setRefuseSeconds(1).build();
    // driver.launchTasks(offerIds, tasks, filters);
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    System.err.println("rescinded");
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    System.err.println("statusUpdate");
    System.out.println("Status update: task " + status.getTaskId().getValue() + " is in state "
        + status.getState());
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId,
      byte[] data) {
    System.err.println("frameworkMessage");
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    System.err.println("disconnected");
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
    System.err.println("slaveLost");
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId,
      int status) {
    System.err.println("executorLost");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    System.err.println("error");
    System.out.println("Error: " + message);
  }

}
