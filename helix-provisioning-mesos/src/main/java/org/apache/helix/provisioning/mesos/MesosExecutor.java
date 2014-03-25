package org.apache.helix.provisioning.mesos;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

public class MesosExecutor implements Executor {

  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
      FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    System.err.println("registered");
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
    System.err.println("reregistered");

  }

  @Override
  public void disconnected(ExecutorDriver driver) {
    System.err.println("disconnected");
  }

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    System.err.println("launchTask");
    new Thread() {
      public void run() {
        try {
          TaskStatus status =
              TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_RUNNING)
                  .build();

          driver.sendStatusUpdate(status);

          System.out.println("Running task " + task.getTaskId());

          // This is where one would perform the requested task.
          Thread.sleep(120000);

          status =
              TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_FINISHED)
                  .build();

          driver.sendStatusUpdate(status);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    System.err.println("killTask");
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {
    System.err.println("frameworkMessage");
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    System.err.println("shutdown");
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    System.err.println("error");
  }

  public static void main(String[] args) {
    MesosExecutorDriver driver = new MesosExecutorDriver(new MesosExecutor());
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }
}
