package org.apache.helix.metamanager.provider.yarn;

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

import org.apache.log4j.Logger;

public class YarnContainerProcess {
	static final Logger log = Logger.getLogger(YarnContainerProcess.class);

	public static void main(String[] args) throws Exception {
		log.trace("BEGIN YarnProcess.main()");
		  
		final String clusterAddress = args[0];
		final String clusterName = args[1];
		final String metadataAddress = args[2];
		final String providerName = args[3];
		final String containerId = args[4];
		
		final ApplicationConfig appConfig = new ApplicationConfig(clusterAddress, clusterName, metadataAddress, providerName);
		
		log.debug("Launching metadata service");
		final ZookeeperMetadataService metaService = new ZookeeperMetadataService(metadataAddress);
		metaService.startService();
		
		log.debug("Launching yarn container service");
		final YarnContainerService yarnProcess = new YarnContainerService(appConfig, metaService, containerId);
		yarnProcess.startService();
		
		log.debug("Installing shutdown hooks");
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				yarnProcess.stopService();
				metaService.stopService();
			}
		}));
		
		System.out.println("Press ENTER to stop container process");
		System.in.read();
		
		log.trace("END YarnProcess.main()");
	}
}
