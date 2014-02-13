package org.apache.helix.model;

import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.alert.AlertAction;
import org.apache.helix.controller.alert.AlertName;
import org.testng.Assert;
import org.testng.annotations.Test;

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

public class TestAlertConfig {
  @Test
  public void testBasic() {
    AlertConfig config = new AlertConfig("default");
    AlertName name =
        new AlertName.Builder().cluster(ClusterId.from("TestCluster")).metric("latency95")
            .largerThan("1000").build();
    AlertAction action =
        new AlertAction.Builder().cmd("enableInstance")
            .args("{cluster}", "{node}", "false").build();
    config.putConfig(name, action);

    AlertName matchingName =
        new AlertName.Builder().cluster(ClusterId.from("TestCluster"))
            .node(ParticipantId.from("localhost_10001")).metric("latency95").largerThan("1000")
            .build();
    AlertAction matchingAction = config.findAlertAction(matchingName);

    Assert.assertNotNull(matchingAction);
    Assert.assertEquals(matchingAction.toString(),
        "(enableInstance)(TestCluster localhost_10001 false)");
  }

  @Test
  public void testFail() {
    AlertConfig config = new AlertConfig("default");
    AlertName name =
        new AlertName.Builder().cluster(ClusterId.from("TestCluster")).metric("latency95")
            .largerThan("1000").build();
    AlertAction action =
        new AlertAction.Builder().cmd("enableInstance")
            .args("{cluster}", "{node}", "false").build();
    config.putConfig(name, action);

    AlertName matchingName =
        new AlertName.Builder().cluster(ClusterId.from("TestCluster"))
            .resource(ResourceId.from("TestDB")).metric("latency95").largerThan("1000").build();
    AlertAction matchingAction = config.findAlertAction(matchingName);
    Assert.assertNull(matchingAction, "Should fail on null scope value for {node}");

  }
}