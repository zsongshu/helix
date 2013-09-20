/*
 * $Id$
 */
package org.apache.helix.integration.task;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.ZkStandAloneCMTestBase;
import org.apache.helix.task.*;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * @author Abe <asebasti@linkedin.com>
 * @version $Revision$
 */
public class TestTaskRebalancerStopResume extends ZkIntegrationTestBase
{
  private static final Logger LOG = Logger.getLogger(ZkStandAloneCMTestBase.class);
  private static final int NUM_NODES = 5;
  private static final int START_PORT = 12918;
  private static final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private static final String TGT_DB = "TestDB";
  private static final String TASK_RESOURCE = "SomeTask";
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_REPLICAS = 3;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private final Map<String, TestHelper.StartCMResult> _startCMResultMap = new HashMap<String, TestHelper.StartCMResult>();
  private HelixManager _manager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass()
      throws Exception
  {
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace))
    {
      _gZkClient.deleteRecursive(namespace);
    }

    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < NUM_NODES; i++)
    {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // Set up target db
    setupTool.addResourceToCluster(CLUSTER_NAME, TGT_DB, NUM_PARTITIONS, MASTER_SLAVE_STATE_MODEL);
    setupTool.rebalanceStorageCluster(CLUSTER_NAME, TGT_DB, NUM_REPLICAS);

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("Reindex", new TaskFactory()
    {
      @Override
      public Task createNewTask(String config)
      {
        return new ReindexTask(config);
      }
    });

    // start dummy participants
    for (int i = 0; i < NUM_NODES; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      TestHelper.StartCMResult result = TestUtil.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName, taskFactoryReg);
      _startCMResultMap.put(instanceName, result);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    TestHelper.StartCMResult startResult = TestHelper.startController(CLUSTER_NAME,
                                                                      controllerName,
                                                                      ZK_ADDR,
                                                                      HelixControllerMain.STANDALONE);
    _startCMResultMap.put(controllerName, startResult);

    // create cluster manager
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    _driver = new TaskDriver(_manager);

    boolean result = ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.MasterNbInExtViewVerifier(ZK_ADDR,
                                                                                                                CLUSTER_NAME));
    Assert.assertTrue(result);

    result = ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                           CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass()
      throws Exception
  {
    /**
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */

    TestHelper.StartCMResult result;
    Iterator<Map.Entry<String, TestHelper.StartCMResult>> it = _startCMResultMap.entrySet().iterator();
    while (it.hasNext())
    {
      String instanceName = it.next().getKey();
      if (instanceName.startsWith(CONTROLLER_PREFIX))
      {
        result = _startCMResultMap.get(instanceName);
        result._manager.disconnect();
        result._thread.interrupt();
        it.remove();
      }
    }

    Thread.sleep(100);
    it = _startCMResultMap.entrySet().iterator();
    while (it.hasNext())
    {
      String instanceName = it.next().getKey();
      result = _startCMResultMap.get(instanceName);
      result._manager.disconnect();
      result._thread.interrupt();
      it.remove();
    }

    _manager.disconnect();
  }

  @Test
  public void stopAndResume()
      throws Exception
  {
    Workflow flow = WorkflowGenerator.generateDefaultSingleTaskWorkflowBuilderWithExtraConfigs(TASK_RESOURCE,
            TaskConfig.COMMAND_CONFIG, String.valueOf(100)).build();

    LOG.info("Starting flow " + flow.getName());
    _driver.start(flow);
    TestUtil.pollForWorkflowState(_manager, TASK_RESOURCE, TaskState.IN_PROGRESS);

    LOG.info("Pausing task");
    _driver.stop(TASK_RESOURCE);
    TestUtil.pollForWorkflowState(_manager, TASK_RESOURCE, TaskState.STOPPED);

    LOG.info("Resuming task");
    _driver.resume(TASK_RESOURCE);
    TestUtil.pollForWorkflowState(_manager, TASK_RESOURCE, TaskState.COMPLETED);
  }

  @Test
  public void stopAndResumeWorkflow()
          throws Exception
  {
    String workflow = "SomeWorkflow";
    Workflow flow = WorkflowGenerator.generateDefaultRepeatedTaskWorkflowBuilder(workflow).build();

    LOG.info("Starting flow " + workflow);
    _driver.start(flow);
    TestUtil.pollForWorkflowState(_manager, workflow, TaskState.IN_PROGRESS);

    LOG.info("Pausing workflow");
    _driver.stop(workflow);
    TestUtil.pollForWorkflowState(_manager, workflow, TaskState.STOPPED);

    LOG.info("Resuming workflow");
    _driver.resume(workflow);
    TestUtil.pollForWorkflowState(_manager, workflow, TaskState.COMPLETED);
  }

  public static class ReindexTask implements Task
  {
    private final long _delay;
    private volatile boolean _canceled;

    public ReindexTask(String cfg)
    {
      _delay = Long.parseLong(cfg);
    }

    @Override
    public TaskResult run()
    {
      long expiry = System.currentTimeMillis() + _delay;
      long timeLeft;
      while (System.currentTimeMillis() < expiry)
      {
        if (_canceled)
        {
          timeLeft = expiry - System.currentTimeMillis();
          return new TaskResult(TaskResult.Status.CANCELED, String.valueOf(timeLeft < 0 ? 0 : timeLeft));
        }
        sleep(50);
      }
      timeLeft = expiry - System.currentTimeMillis();
      return new TaskResult(TaskResult.Status.COMPLETED, String.valueOf(timeLeft < 0 ? 0 : timeLeft));
    }

    @Override
    public void cancel()
    {
      _canceled = true;
    }

    private static void sleep(long d)
    {
      try
      {
        Thread.sleep(d);
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
    }
  }
}
