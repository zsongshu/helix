package org.apache.helix.integration;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Date;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestExceptionInStateModelReset extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestExceptionInStateModelReset.class);

  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE", "ERROR" })
  public static class TestMSStateModel extends StateModel 
  {
    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become SLAVE from OFFLINE");
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become MASTER from SLAVE");
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become SLAVE from MASTER");
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become OFFLINE from SLAVE");
    }
    
    public void reset()
    {
      LOG.warn("Reset");
      throw new RuntimeException("IGNORABLE: test throwing exception from StateModel#reset()");
    }
  }
  
  public static class TestMSStateModelFactory extends StateModelFactory<TestMSStateModel> 
  {

    @Override
    public TestMSStateModel createNewStateModel(String partitionName)
    {
      TestMSStateModel model = new TestMSStateModel();
      return model;
    }
    
  }
  
  @Test
  public void test() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            32, // partitions per resource
                            n, // number of nodes
                            2, // replicas
                            "MasterSlave",
                            true); // do rebalance
    
    
    ClusterController controller =
        new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0)
      {
        StateModelFactory factory = new TestMSStateModelFactory();
        participants[i] = new MockParticipant(factory, clusterName, instanceName, ZK_ADDR, null);
      }
      else
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      }
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // stop participant[0], will throw runtime exception in reset()
    participants[0].syncStop();
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    LiveInstance liveInsance = accessor.getProperty(keyBuilder.liveInstance(participants[0].getInstanceName()));
    Assert.assertNull(liveInsance);
    
    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 1; i < n; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
