package org.apache.helix.mock.participant;

import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;

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


// mock master slave state model factory
public class MockMSModelFactory extends StateTransitionHandlerFactory<MockMSStateModel> {
  private MockTransition _transition;

  public MockMSModelFactory() {
    this(null);
  }

  public MockMSModelFactory(MockTransition transition) {
    _transition = transition;
  }

  public void setTrasition(MockTransition transition) {
    _transition = transition;

    // set existing transition
    for (PartitionId partition : getPartitionSet()) {
      MockMSStateModel stateModel = getTransitionHandler(partition);
      stateModel.setTransition(transition);
    }
  }

  @Override
  public MockMSStateModel createStateTransitionHandler(PartitionId partitionKey) {
    MockMSStateModel model = new MockMSStateModel(_transition);

    return model;
  }
}
