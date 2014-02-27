package org.apache.helix.monitoring;

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
import org.apache.helix.MonitoringTestHelper;
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
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpExchange;
import org.eclipse.jetty.io.ByteArrayBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRiemannAlertProxy extends ZkUnitTestBase {
  void sendAlert(int proxyPort, String alertNameStr) throws Exception {
    HttpClient client = new HttpClient();
    client.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
    client.start();

    ContentExchange exchange = new ContentExchange(true);
    exchange.setMethod("POST");
    exchange.setURL("http://localhost:" + proxyPort);
    exchange.setRequestContent(new ByteArrayBuffer(alertNameStr));

    client.send(exchange);

    // Waits until the exchange is terminated
    int exchangeState = exchange.waitForDone();
    Assert.assertTrue(exchangeState == HttpExchange.STATUS_COMPLETED);

    client.stop();
  }

  @Test
  public void testBasic() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName);

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    int proxyPort = MonitoringTestHelper.availableTcpPort();

    RiemannAlertProxy proxy = new RiemannAlertProxy(proxyPort, baseAccessor);

    proxy.start();

    // Send a valid alert
    String alertNameStr = String.format("(%s.%%.node1)(latency95)>(1000)", clusterName);
    sendAlert(proxyPort, alertNameStr);

    // Send an invalid alert
    String inValidAlertNameStr = "IGNORABLE: invalid alert";
    sendAlert(proxyPort, inValidAlertNameStr);

    // Check only 1 alert controller message is sent
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<Message> messages = accessor.getChildValues(keyBuilder.controllerMessages());

    Assert.assertEquals(messages.size(), 1);
    Message message = messages.get(0);
    Assert.assertEquals(message.getMsgType(), MessageType.ALERT.toString());
    Assert.assertEquals(message.getAttribute(Attributes.ALERT_NAME), alertNameStr);

    proxy.shutdown();
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
