package org.apache.helix.controller.alert;

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

import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.alert.AlertScope.AlertScopeField;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAlertName {

  @Test
  public void testBasic() {
    AlertName.Builder builder = new AlertName.Builder();
    AlertName name =
        builder.cluster(ClusterId.from("TestCluster")).node(ParticipantId.from("localhost_12918"))
            .resource(ResourceId.from("TestDB")).partitionId(PartitionId.from("TestDB_0"))
            .metric("latency95").largerThan("1000").build();
    Assert.assertEquals(name.toString(),
        "(TestCluster.%.localhost_12918.TestDB.TestDB_0)(latency95)>(1000)");

    name = AlertName.from("(TestCluster.%.localhost_12918.TestDB.TestDB_0)(latency95)>(1000)");
    Assert.assertEquals(name._scope.getClusterId(), ClusterId.from("TestCluster"));
    Assert.assertNull(name._scope.get(AlertScopeField.tenant));
    Assert.assertEquals(name._scope.get(AlertScopeField.node),
        ParticipantId.from("localhost_12918"));
    Assert.assertEquals(name._scope.get(AlertScopeField.resource), ParticipantId.from("TestDB"));
    Assert.assertEquals(name._scope.get(AlertScopeField.partition), ParticipantId.from("TestDB_0"));
    Assert.assertEquals(name._comparator, AlertComparator.LARGER);
    Assert.assertEquals(name._metric, "latency95");
    Assert.assertEquals(name._value, "1000");
  }

  @Test
  public void testInvalidAlertNameString() {
    try {
      AlertName.from("(TestCluster.%.localhost_12918.TestDB.TestDB_0)(latency95)><(1000)");
      Assert.fail("Should fail on invalid comparator ><");
    } catch (IllegalArgumentException e) {
      // ok
    }

    try {
      AlertName.from("(TestCluster.%.localhost_12918.TestDB.TestDB_0)()(latency95)>()(1000)");
      Assert.fail("Should fail on invalid alert name format");
    } catch (IllegalArgumentException e) {
      // ok
    }

    try {
      AlertName.from("(TestCluster.%.localhost_12918.TestDB.TestDB_0)>(1000)");
      Assert.fail("Should fail on missing metric field");
    } catch (IllegalArgumentException e) {
      // ok
    }

    try {
      AlertName.from("(TestCluster.%.localhost_12918.TestDB.TestDB_0.blabla)(latency95)>(1000)");
      Assert.fail("Should fail on invalid too many scope arguments");
    } catch (IllegalArgumentException e) {
      // ok
    }
  }

  @Test
  public void testMatch() {
    AlertName.Builder builder;
    AlertName matchingName;
    boolean match;

    builder = new AlertName.Builder();
    AlertName name =
        builder.cluster(ClusterId.from("TestCluster")).resource(ResourceId.from("TestDB"))
            .metric("latency95").largerThan("1000").build();

    builder = new AlertName.Builder();
    matchingName =
        builder.cluster(ClusterId.from("TestCluster")).node(ParticipantId.from("localhost_12918"))
            .resource(ResourceId.from("TestDB")).metric("latency95").largerThan("1000").build();
    match = name.match(matchingName);
    Assert.assertTrue(match);

    builder = new AlertName.Builder();
    matchingName =
        builder.cluster(ClusterId.from("TestCluster")).node(ParticipantId.from("localhost_12918"))
            .resource(ResourceId.from("MyDB")).metric("latency95").largerThan("1000").build();
    match = name.match(matchingName);
    Assert.assertFalse(match);
  }
}