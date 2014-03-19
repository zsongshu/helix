package org.apache.helix.monitoring.riemann;

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
import java.util.List;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.monitoring.riemann.HelixAlertMessenger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixAlertMessenger extends ZkUnitTestBase {

  @Test
  public void testBasic() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName);

    HelixAlertMessenger messenger = new HelixAlertMessenger(ZK_ADDR);

    // Send a valid alert
    String alertNameStr = String.format("(%s.%%.node1)(latency95)>(1000)", clusterName);
    messenger.onAlert(alertNameStr);

    // Send an invalid alert
    String inValidAlertNameStr = "IGNORABLE: invalid alert";
    messenger.onAlert(inValidAlertNameStr);

    // Check only 1 alert controller message is sent
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<Message> messages = accessor.getChildValues(keyBuilder.controllerMessages());

    Assert.assertEquals(messages.size(), 1);
    Message message = messages.get(0);
    Assert.assertEquals(message.getMsgType(), MessageType.ALERT.toString());
    Assert.assertEquals(message.getAttribute(Attributes.ALERT_NAME), alertNameStr);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testThrottle() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName);

    HelixAlertMessenger messenger = new HelixAlertMessenger(ZK_ADDR);

    long startT = System.currentTimeMillis();
    String alertNameStr = String.format("(%s.%%.node1)(latency95)>(1000)", clusterName);
    for (int i = 0; i < 10; i++) {
      messenger.onAlert(alertNameStr);
    }
    long seconds = (System.currentTimeMillis() - startT) / 1000 + 1;

    // Check no more than 1 alert per second
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<Message> messages = accessor.getChildValues(keyBuilder.controllerMessages());

    Assert.assertTrue(messages.size() <= seconds, "Should not receive more than " + seconds
        + " messages");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
