package org.apache.helix.manager.zk;

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
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.controller.alert.AlertAction;
import org.apache.helix.model.AlertConfig;
import org.apache.helix.controller.alert.AlertName;
import org.apache.helix.model.Error;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDefaultAlertMsgHandler extends ZkUnitTestBase {

  @Test
  public void testBasic() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    MessageHandlerFactory fty = new DefaultAlertMsgHandlerFactory();
    controller.getMessagingService().registerMessageHandlerFactory(fty.getMessageType(), fty);
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);

    // add alert config
    final HelixDataAccessor accessor = controller.getHelixDataAccessor();
    AlertConfig config = new AlertConfig("default");
    AlertName name =
        new AlertName.Builder().cluster(ClusterId.from(clusterName)).metric("latency95")
            .largerThan("1000").build();
    AlertAction action =
        new AlertAction.Builder().cmd("enableInstance")
            .args("{cluster}", "{node}", "false").build();
    config.putConfig(name, action);
    final PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    accessor.setProperty(keyBuilder.alertConfig("default"), config);

    Message alertMsg;

    // test send an alert message that matches the config
    alertMsg = new Message(MessageType.ALERT, MessageId.from("msg_1"));
    alertMsg.setAttribute(Message.Attributes.ALERT_NAME,
        String.format("(%s.%s.%s)(latency95)>(1000)", clusterName, "tenant1", "localhost_12918"));
    alertMsg.setTgtSessionId(controller.getSessionId());
    alertMsg.setTgtName("controller");
    accessor.setProperty(keyBuilder.controllerMessage(alertMsg.getId()), alertMsg);

    // verify localhost_12918 is disabled
    result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig("localhost_12918"));
        return !config.getInstanceEnabled();
      }
    }, 10 * 1000);
    Assert.assertTrue(result);

    // test send an alert message that doesn't match any config
    alertMsg = new Message(MessageType.ALERT, MessageId.from("msg_2"));
    alertMsg.setAttribute(Message.Attributes.ALERT_NAME,
        String.format("(%s.%s.%s)(errorRatio)>(0.1)", clusterName, "tenant1", "localhost_12918"));
    alertMsg.setTgtSessionId(controller.getSessionId());
    alertMsg.setTgtName("controller");
    accessor.setProperty(keyBuilder.controllerMessage(alertMsg.getId()), alertMsg);
    result = containsCtrlErrMsg(accessor, "msg_2");
    Assert.assertTrue(result);

    // test send an invalid alert message that doesn't have {node} scope
    alertMsg = new Message(MessageType.ALERT, MessageId.from("msg_3"));
    alertMsg.setAttribute(Message.Attributes.ALERT_NAME,
        String.format("(%s.%s.%%.%s)(latency95)>(1000)", clusterName, "tenant1", "TestDB"));
    alertMsg.setTgtSessionId(controller.getSessionId());
    alertMsg.setTgtName("controller");
    accessor.setProperty(keyBuilder.controllerMessage(alertMsg.getId()), alertMsg);
    result = containsCtrlErrMsg(accessor, "msg_3");
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  /**
   * verify we have an error entry for msgId in CONTROLLER/ERRORS/ALERT
   * @param msgId
   * @return
   */
  private boolean containsCtrlErrMsg(final HelixDataAccessor accessor, final String msgId)
      throws Exception {
    boolean result = TestHelper.verify(new TestHelper.Verifier() {
      final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

      @Override
      public boolean verify() throws Exception {
        Error error =
            accessor.getProperty(keyBuilder.controllerTaskError(MessageType.ALERT.name()));
        if (error == null) {
          return false;
        }

        for (String key : error.getRecord().getMapFields().keySet()) {
          if (!key.startsWith("HELIX_ERROR")) {
            continue;
          }
          String tmpMsgId = error.getRecord().getMapField(key).get("MSG_ID");
          if (tmpMsgId != null && tmpMsgId.equals(msgId)) {
            return true;
          }
        }
        return false;
      }
    }, 10 * 1000);

    return result;
  }
}