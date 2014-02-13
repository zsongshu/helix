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
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.alert.AlertScope.AlertScopeField;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAlertScope {

  @Test
  public void testBasic() {
    AlertScope scope =
        new AlertScope(ClusterId.from("TestCluster"), null, ParticipantId.from("localhost_12918"),
            ResourceId.from("TestDB"), null);

    Assert.assertEquals(scope.getClusterId(), ClusterId.from("TestCluster"));
    Assert.assertEquals(scope.get(AlertScopeField.node), ParticipantId.from("localhost_12918"));
    Assert.assertEquals(scope.get(AlertScopeField.resource), ResourceId.from("TestDB"));
    Assert.assertEquals(scope.toString(), "TestCluster.%.localhost_12918.TestDB.%");

    AlertScope matchingScope = new AlertScope("TestCluster", "Tenant1", "localhost_12918");
    boolean match = scope.match(matchingScope);
    Assert.assertTrue(match);

  }

  @Test
  public void testEmptyClusterId() {
    AlertScope scope = new AlertScope((String[]) null);
    Assert.assertEquals(scope.toString(), "%.%.%.%.%");

    scope = new AlertScope(new String[] {});
    Assert.assertEquals(scope.toString(), "%.%.%.%.%");

    scope = new AlertScope(null, "Tenant1", "localhost_12918");
    Assert.assertEquals(scope.toString(), "%.Tenant1.localhost_12918.%.%");
  }

  @Test
  public void testWildcard() {
    AlertScope scope = new AlertScope("%");
    Assert.assertEquals(scope.toString(), "%.%.%.%.%");

    scope = new AlertScope("TestCluster", "%", "localhost_12918");
    Assert.assertEquals(scope.toString(), "TestCluster.%.localhost_12918.%.%");
  }

  @Test
  public void testTooManyArguments() {
    try {
      new AlertScope("TestCluster", null, "localhost_12918", "TestDB", "TestDB_0", "blabla");
      Assert.fail("Should fail on too many arguments");
    } catch (IllegalArgumentException e) {
      // ok
    }
  }
}
