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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.log4j.Logger;

public class ManagerProcess
{
	static final Logger log = Logger.getLogger(ManagerProcess.class);
	
  final String clusterName;
  final String zkAddress;
  final String instanceName;
  final ClusterContainerProvider provider;
  final ClusterAdmin admin;
  
  HelixManager participantManager;
  
  ManagerProcess(String clusterName, String zkAddress, String instanceName, ClusterContainerProvider provider, ClusterAdmin admin)
  {
    this.clusterName = clusterName;
    this.zkAddress = zkAddress;
    this.instanceName = instanceName;
    this.provider = provider;
    this.admin = admin;
  }

  public void start() throws Exception
  {
    log.info("STARTING "+ instanceName);
    participantManager = HelixManagerFactory.getZKHelixManager(clusterName,
        instanceName, InstanceType.PARTICIPANT, zkAddress);
    participantManager.getStateMachineEngine().registerStateModelFactory(
        "OnlineOffline", new ManagerFactory(provider, admin));
    participantManager.connect();
    log.info("STARTED "+ instanceName);

  }

  public void stop()
  {
    if (participantManager != null)
    {
      participantManager.disconnect();
    }
  }
}
