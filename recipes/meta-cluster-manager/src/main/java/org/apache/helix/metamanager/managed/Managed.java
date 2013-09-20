package org.apache.helix.metamanager.managed;

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

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

@StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "SLAVE", "MASTER", "DROPPED" })
public class Managed extends StateModel {
	
	static final Logger log = Logger.getLogger(Managed.class);
	
	@Transition(from = "OFFLINE", to = "SLAVE")
	public void offlineToSlave(Message m, NotificationContext context) {
		log.trace(String.format("%s transitioning from OFFLINE to SLAVE",
				context.getManager().getInstanceName()));
	}

	@Transition(from = "SLAVE", to = "OFFLINE")
	public void slaveToOffline(Message m, NotificationContext context) {
		log.trace(String.format("%s transitioning from SLAVE to OFFLINE",
				context.getManager().getInstanceName()));
	}

	@Transition(from = "SLAVE", to = "MASTER")
	public void slaveToMaster(Message m, NotificationContext context) {
		log.trace(String.format("%s transitioning from SLAVE to MASTER",
				context.getManager().getInstanceName()));
	}

	@Transition(from = "MASTER", to = "SLAVE")
	public void masterToSlave(Message m, NotificationContext context) {
		log.trace(String.format("%s transitioning from MASTER to SLAVE",
				context.getManager().getInstanceName()));
	}

	@Transition(from = "OFFLINE", to = "DROPPED")
	public void offlineToDropped(Message m, NotificationContext context) {
		log.trace(String.format("%s transitioning from OFFLINE to DROPPED",
				context.getManager().getInstanceName()));
	}

}
