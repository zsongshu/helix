package org.apache.helix.metamanager;

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

@StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "ONLINE" })
public class Manager extends StateModel {
	
	static final Logger log = Logger.getLogger(Manager.class);
	
	ClusterContainerProvider provider;
	ClusterAdmin admin;

	public Manager(ClusterContainerProvider provider, ClusterAdmin admin) {
		this.provider = provider;
		this.admin = admin;
	}

	@Transition(from = "OFFLINE", to = "ONLINE")
	public void acquire(Message m, NotificationContext context) throws Exception {
		String containerType = m.getResourceName();
		String containerId = m.getPartitionName();
		String instanceId = context.getManager().getInstanceName();
		
		log.trace(String.format("%s:%s transitioning from OFFLINE to ONLINE",
				containerId, instanceId));
		
		bestEffortRemove(containerId);
		
		// add instance to cluster
		admin.addInstance(containerId);
		
		// create container
		provider.create(containerId, containerType);

		try {
			admin.rebalance();
		} catch (Exception e) {
			// ignore
			log.warn(String.format("rebalancing cluster failed (error='%s')", e.getMessage()));
		}

		log.info(String.format("%s acquired container '%s' (type='%s')",
				instanceId, containerId, containerType));
	}

	@Transition(from = "ONLINE", to = "OFFLINE")
	public void release(Message m, NotificationContext context) {
		String containerId = m.getPartitionName();
		String instanceId = context.getManager().getInstanceName();

		log.trace(String.format("%s:%s transitioning from ONLINE to OFFLINE",
				containerId, instanceId));
		
		bestEffortRemove(containerId);
		
		try {
			admin.rebalance();
		} catch (Exception e) {
			// ignore
			log.warn(String.format("rebalancing cluster failed (error='%s')", e.getMessage()));
		}

		log.info(String.format("%s destroyed container '%s'",
				instanceId, containerId));

	}

	@Transition(from = "ERROR", to = "OFFLINE")
	public void recover(Message m, NotificationContext context) {
		String containerId = m.getPartitionName();
		String instanceId = context.getManager().getInstanceName();

		log.trace(String.format("%s:%s transitioning from ERROR to OFFLINE",
				containerId, instanceId));
	}
	
	@Transition(from = "OFFLINE", to = "DROPPED")
	public void drop(Message m, NotificationContext context) {
		String containerId = m.getPartitionName();
		String instanceId = context.getManager().getInstanceName();

		log.trace(String.format("%s:%s transitioning from OFFLINE to DROPPED",
				containerId, instanceId));
	}
	
	private void bestEffortRemove(String containerId) {
		log.debug(String.format("Best effort removal of container '%s'", containerId));
		
		try {
			provider.destroy(containerId);
			log.debug(String.format("Container '%s' destroyed", containerId));
		} catch (Exception e) {
			log.debug(String.format("Container '%s' does not exist", containerId));
		}
		
		try {
			admin.removeInstance(containerId);
			log.debug(String.format("Instance '%s' removed", containerId));
		} catch (Exception e) {
			log.debug(String.format("Instance '%s' does not exist", containerId));
		}
		
	}
	
}
