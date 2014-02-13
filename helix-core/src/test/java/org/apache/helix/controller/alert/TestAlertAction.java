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

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAlertAction {
  @Test
  public void testBasic() throws Exception {
    AlertAction.Builder builder = new AlertAction.Builder();

    AlertAction action =
        builder.cmd("enableInstance").args("TestCluster", "{node}", "false").build();
    Assert.assertNotNull(action.getCliArgs());
    Assert.assertEquals(action.getCliArgs().length, 4);
    Assert.assertEquals(action.getCliArgs()[0], "enableInstance");
    Assert.assertEquals(action.getCliArgs()[1], "TestCluster");
    Assert.assertEquals(action.getCliArgs()[2], "{node}");
    Assert.assertEquals(action.getCliArgs()[3], "false");
    Assert.assertEquals(action.toString(), "(enableInstance)(TestCluster {node} false)");

    action = AlertAction.from("(enableInstance)(TestCluster {node} true)");
    Assert.assertNotNull(action.getCliArgs());
    Assert.assertEquals(action.getCliArgs().length, 4);
    Assert.assertEquals(action.getCliArgs()[0], "enableInstance");
    Assert.assertEquals(action.getCliArgs()[1], "TestCluster");
    Assert.assertEquals(action.getCliArgs()[2], "{node}");
    Assert.assertEquals(action.getCliArgs()[3], "true");
  }

  @Test
  public void testInvalidActionStr() {
    try {
      AlertAction.from("(invalidCmd)(TestCluster {node} true)");
      Assert.fail("Should fail on invalid HelixAdminCmd");
    } catch (IllegalArgumentException e) {
      // ok
    }

    try {
      AlertAction.from("(enableInstance)(TestCluster {invalidScope} true)");
      Assert.fail("Should fail on invalid alertScope");
    } catch (IllegalArgumentException e) {
      // ok
    }

    try {
      AlertAction.from("(enableInstance)()(TestCluster {node} true)");
      Assert.fail("Should fail on invalid alertAction format");
    } catch (IllegalArgumentException e) {
      // ok
    }

  }

  @Test
  public void testInvaidConstructorArgument() {
    try {
      new AlertAction(null, new String[] {
          "TestCluster", "{node}", "true)"
      });
      Assert.fail("Should fail on null HelixAdminCmd");
    } catch (NullPointerException e) {
      // ok
    }

    try {
      new AlertAction("enableInstance", new String[] {
          "TestCluster", "{invalidScope}", "true)"
      });
      Assert.fail("Should fail on invalid alert scope");
    } catch (IllegalArgumentException e) {
      // ok
    }

  }
}
