/*
 * $Id$
 */
package org.apache.helix.integration.task;


import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkHelixTestManager;
import org.apache.helix.mock.participant.DummyProcess;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.*;
import org.apache.log4j.Logger;
import org.testng.Assert;


/**
 * Static test utility methods.
 *
 * @author Abe <asebasti@linkedin.com>
 * @version $Revision$
 */
public class TestUtil
{
  private static final Logger LOG = Logger.getLogger(TestUtil.class);

  public static TestHelper.StartCMResult startDummyProcess(String zkAddr,
                                                            String clusterName,
                                                            String instanceName,
                                                            Map<String, TaskFactory> taskFactoryMap)
      throws Exception
  {
    TestHelper.StartCMResult result = new TestHelper.StartCMResult();
    ZkHelixTestManager manager = new ZkHelixTestManager(clusterName,
                                                        instanceName,
                                                        InstanceType.PARTICIPANT,
                                                        zkAddr);
    result._manager = manager;
    Thread thread = new Thread(new MockInstanceThread(manager, instanceName, taskFactoryMap));
    result._thread = thread;
    thread.start();

    return result;
  }

  /**
   * Polls {@link org.apache.helix.task.TaskContext} for given task resource until a timeout is reached.
   * If the task has not reached target state by then, an error is thrown
   *
   * @param workflowResource Resource to poll for completeness
   * @throws InterruptedException
   */
  public static void pollForWorkflowState(HelixManager manager, String workflowResource, TaskState state)
          throws InterruptedException
  {
    // Wait for completion.
    long st = System.currentTimeMillis();
    WorkflowContext ctx;
    do
    {
      Thread.sleep(100);
      ctx = TaskUtil.getWorkflowContext(manager, workflowResource);
    }
    while ((ctx == null || ctx.getWorkflowState() == null || ctx.getWorkflowState() != state)
            && System.currentTimeMillis() < st + 2 * 60 * 1000 /* 2 mins */);

    Assert.assertNotNull(ctx);
    Assert.assertEquals(ctx.getWorkflowState(), state);
  }

  public static void pollForTaskState(HelixManager manager, String workflowResource, String taskName, TaskState state)
          throws InterruptedException
  {
    // Wait for completion.
    long st = System.currentTimeMillis();
    WorkflowContext ctx;
    do
    {
      Thread.sleep(100);
      ctx = TaskUtil.getWorkflowContext(manager, workflowResource);
    }
    while ((ctx == null || ctx.getTaskState(taskName) == null || ctx.getTaskState(taskName) != state)
            && System.currentTimeMillis() < st + 2 * 60 * 1000 /* 2 mins */);

    Assert.assertNotNull(ctx);
    Assert.assertEquals(ctx.getWorkflowState(), state);
  }

  private static class MockInstanceThread implements Runnable
  {
    private final HelixManager _manager;
    private final String _instanceName;
    private final Map<String, TaskFactory> _factoryMap;

    public MockInstanceThread(HelixManager manager, String instanceName, Map<String, TaskFactory> factoryMap)
    {
      _manager = manager;
      _instanceName = instanceName;
      _factoryMap = factoryMap;
    }

    @Override
    public void run()
    {
      try
      {
        StateMachineEngine stateMach = _manager.getStateMachineEngine();
        // Register dummy MasterSlave state model factory.
        stateMach.registerStateModelFactory("MasterSlave", new DummyProcess.DummyStateModelFactory(0));
        // Register a Task state model factory.
        stateMach.registerStateModelFactory("Task", new TaskStateModelFactory(_manager, _factoryMap));

        _manager.connect();
        Thread.currentThread().join();
      }
      catch (InterruptedException e)
      {
        LOG.info("participant:" + _instanceName + ", " + Thread.currentThread().getName() + " interrupted");
      }
      catch (Exception e)
      {
        LOG.error("participant:" + _instanceName + ", " + Thread.currentThread().getName() + " interrupted", e);
      }
    }
  }
}
