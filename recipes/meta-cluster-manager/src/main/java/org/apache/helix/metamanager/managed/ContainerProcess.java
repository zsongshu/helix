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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.log4j.Logger;

public class ContainerProcess
{
	static final Logger log = Logger.getLogger(ContainerProcess.class);
	
  private String clusterName;
  private String zkAddress;
  private String instanceName;
  private HelixManager participantManager;

 public ContainerProcess(String clusterName, String zkAddress, String instanceName)
  {
    this.clusterName = clusterName;
    this.zkAddress = zkAddress;
    this.instanceName = instanceName;

  }

  public void start() throws Exception
  {
    log.info("STARTING "+ instanceName);
    participantManager = HelixManagerFactory.getZKHelixManager(clusterName,
        instanceName, InstanceType.PARTICIPANT, zkAddress);
    participantManager.getStateMachineEngine().registerStateModelFactory(
        "MasterSlave", new ManagedFactory());
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

  public static void main(String[] args) throws Exception
  {
    final String zkAddress = args[0];
    final String clusterName = args[1];
    final String instanceName = args[2];

    // Give a unique id to each process, most commonly used format hostname_port
    final ContainerProcess managerProcess = new ContainerProcess(clusterName, zkAddress,
        instanceName);
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
    	  log.info("Shutting down " + instanceName);
        managerProcess.stop();
      }
    });
    managerProcess.start();
    Thread.currentThread().join();
  }
}
