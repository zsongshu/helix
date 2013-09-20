package org.apache.helix.metamanager.yarn;

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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.metamanager.managed.ManagedFactory;
import org.apache.helix.metamanager.yarn.ContainerMetadata.ContainerState;
import org.apache.log4j.Logger;

public class YarnProcess {
	static final Logger log = Logger.getLogger(YarnProcess.class);

	static final long CONTAINERSERVICE_INTERVAL = 1000;

	final ApplicationConfig appConfig;
	final String containerId;
	
	HelixManager participantManager;

	MetadataService metaService;
	ScheduledExecutorService executor;


	public YarnProcess(ApplicationConfig appConfig, String containerId) {
		this.appConfig = appConfig;
		this.containerId = containerId;
	}

	public void startService() {
		log.info(String.format("start metadata service for '%s'", containerId));
		metaService = new MetadataService(appConfig);
		metaService.start();
		
		executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(new ContainerService(), 0, CONTAINERSERVICE_INTERVAL, TimeUnit.MILLISECONDS);
	}

	public void stopService() {
		log.info(String.format("stop metadata service for '%s'", containerId));
		if (metaService != null) {
			metaService.stop();
			metaService = null;
		}
		
		if(executor != null) {
			executor.shutdown();
		}
	}
	
	public boolean isRunning() {
		if(executor == null)
			return false;
		return !executor.isTerminated();
	}
	
	public void startParticipant() throws Exception {
		log.info("STARTING " + containerId);
		participantManager = HelixManagerFactory.getZKHelixManager(appConfig.clusterName,
				containerId, InstanceType.PARTICIPANT, appConfig.clusterAddress);
		participantManager.getStateMachineEngine().registerStateModelFactory(
				"MasterSlave", new ManagedFactory());
		participantManager.connect();
		log.info("STARTED " + containerId);
	}

	public void stopParticipant() {
		if (participantManager != null) {
			participantManager.disconnect();
			participantManager = null;
		}
	}
	
	public void updateContainerStatus() {
		log.info("updating container status");
		try {
			ContainerMetadata meta = metaService.read(containerId);
			
			if(meta.state == ContainerState.CONNECTING) {
				log.info("container connecting, going to active");
				try {
					startParticipant();
					metaService.update(new ContainerMetadata(meta, ContainerState.ACTIVE));
				} catch (Exception e) {
					log.error("Failed to start participant, going to failed", e);
					stopParticipant();
					metaService.update(new ContainerMetadata(meta, ContainerState.FAILED));
				}
			}
			
			if(meta.state == ContainerState.ACTIVE) {
				// do something
				// and go to failed on error
			}
			
			if(meta.state == ContainerState.TEARDOWN) {
				log.info("container teardown, going to halted");
				stopParticipant();
				metaService.update(new ContainerMetadata(meta, ContainerState.HALTED));
				stopService();
			}
			
		} catch(Exception e) {
			log.warn(String.format("Container '%s' does not exist, stopping service", containerId));
			stopService();
		}
	}
	
	class ContainerService implements Runnable {
		@Override
		public void run() {
			updateContainerStatus();
		}
	}

  public static void main(String[] args) throws Exception
  {
	log.trace("BEGIN YarnProcess.main()");
	  
    final String clusterAddress = args[0];
    final String clusterName = args[1];
    final String providerAddress = args[2];
    final String providerName = args[3];
    final String containerId = args[4];

    final ApplicationConfig appConfig = new ApplicationConfig(clusterAddress, clusterName, providerAddress, providerName);
    
    final YarnProcess yarnProcess = new YarnProcess(appConfig, containerId);

    yarnProcess.startService();
    
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
		@Override
		public void run() {
			yarnProcess.stopService();
		}
	}));
    
	while(yarnProcess.isRunning()) {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// ignore
		}
	}
	
	log.trace("END YarnProcess.main()");
  }
}
