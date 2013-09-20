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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.metamanager.container.ContainerUtils;
import org.apache.helix.metamanager.impl.StaticTargetProvider;
import org.apache.helix.metamanager.impl.local.LocalContainerProvider;
import org.apache.helix.metamanager.impl.local.LocalStatusProvider;
import org.apache.helix.metamanager.impl.shell.ShellContainerProvider;
import org.apache.helix.metamanager.impl.shell.ShellStatusProvider;
import org.apache.helix.metamanager.impl.yarn.YarnApplicationProperties;
import org.apache.helix.metamanager.impl.yarn.YarnContainerProvider;
import org.apache.helix.metamanager.impl.yarn.YarnStatusProvider;
import org.apache.helix.metamanager.provider.ProviderProcess;
import org.apache.helix.metamanager.provider.ProviderRebalancer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

public class MetaManagerDemo
{
	static final long TIMESTEP_INTERVAL = 1000;
	
	static final String PROVIDER_LOCAL = "LOCAL";
	static final String PROVIDER_SHELL = "SHELL";
	static final String PROVIDER_YARN  = "YARN";
	
	static final Logger log = Logger.getLogger(MetaManagerDemo.class);
	
	static final int zkPort = 2199;
	static final String zkAddress = "localhost:" + zkPort;
	static final String metaClusterName = "meta-cluster";
	static final String managedClusterName = "managed-cluster";
	static final String metaResourceName = "container";
	static final String managedResourceName = "database";
	
	static final int numContainerProviders = 3;
	static final int numContainerMax = 7;
	static final int numContainerMin = 3;
	static final int numContainerStep = 2;
	static final int numContainerReplica = 1;
	
	static final int numManagedPartitions = 10;
	static final int numManagedReplica = 2;
	
	static List<ContainerProvider> providers = new ArrayList<ContainerProvider>();
	static int providerCount = 0;
	
	static Collection<Service> services = new ArrayList<Service>();
	
  /**
   * LockManagerDemo clusterName, numInstances, lockGroupName, numLocks
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
	  
	String containerProviderType = PROVIDER_LOCAL;
	if(args.length >= 1) {
		containerProviderType = args[0];
	}
	  
    StaticTargetProvider targetProvider = null;
    ProviderProcess[] managerProcesses = new ProviderProcess[numContainerProviders];

    HelixManager metaControllerManager = null;
    HelixManager managedControllerManager = null;
    
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Destroying containers");
                for (ContainerProvider provider : providers) {
                    provider.destroyAll();
                }
                log.info("Stopping services");
                for (Service service : services) {
                    try { service.stop(); } catch (Exception ignore) {}
                }
            }
        }));

    try
    {
      log.info("Starting ZooKeeper");
      startLocalZookeeper();
      HelixAdmin admin = new ZKHelixAdmin(zkAddress);

      log.info("Create clusters");
      admin.addCluster(metaClusterName, true);
      admin.addCluster(managedClusterName, true);
      
      log.info("Create providers");
      targetProvider = startService(new StaticTargetProvider(Collections.singletonMap(metaResourceName, numContainerMin)));
      StatusProvider statusProvider = startService(createContainerStatusProvider(containerProviderType));
      
      log.info("Setup config tool");
      ConfigTool.setClusterStatusProvider(targetProvider);
      ConfigTool.setContainerStatusProvider(statusProvider);
      
      // Managed Cluster
      log.info("Setup managed cluster");
      admin.addStateModelDef(managedClusterName, "MasterSlave",
          new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave()));
      admin.addResource(managedClusterName, managedResourceName, numManagedPartitions,
          "MasterSlave", RebalanceMode.FULL_AUTO.toString());
      
      IdealState managedIdealState = admin.getResourceIdealState(managedClusterName, managedResourceName);
      managedIdealState.setInstanceGroupTag(metaResourceName);
      managedIdealState.setReplicas(String.valueOf(numManagedReplica));
      admin.setResourceIdealState(managedClusterName, managedResourceName, managedIdealState);	  
      
      // Meta Cluster
      log.info("Setup meta cluster");
      admin.addStateModelDef(metaClusterName, "OnlineOffline",
          new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));
      admin.addResource(metaClusterName, metaResourceName, targetProvider.getTargetContainerCount(metaResourceName),
          "OnlineOffline", RebalanceMode.USER_DEFINED.toString());
      
      IdealState metaIdealState = admin.getResourceIdealState(metaClusterName, metaResourceName);
      metaIdealState.setRebalancerClassName(ProviderRebalancer.class.getName());
      metaIdealState.setReplicas("1");
      admin.setResourceIdealState(metaClusterName, metaResourceName, metaIdealState);	  
      
      log.info("Starting meta processes (container providers)");
      for (int i = 0; i < numContainerProviders; i++)
      {
        String instanceName = "provider_" + i;
        admin.addInstance(metaClusterName, new InstanceConfig(instanceName));
        
        ClusterAdmin clusterAdmin = new HelixClusterAdmin(managedClusterName, admin);

        managerProcesses[i] = new ProviderProcess(metaClusterName, zkAddress,
                instanceName, startService(createContainerProvider(containerProviderType)), clusterAdmin);
        managerProcesses[i].start();
      }
      
      log.info("Starting managed cluster controller");
      managedControllerManager = HelixControllerMain.startHelixController(zkAddress,
          managedClusterName, "managedController", HelixControllerMain.STANDALONE);
      log.info("Starting meta cluster controller");
      metaControllerManager = HelixControllerMain.startHelixController(zkAddress,
          metaClusterName, "metaController", HelixControllerMain.STANDALONE);
          
      waitUntilRebalancedCount(numContainerMin, admin);
      printStep("Initial cluster state", admin);
      
      while(targetProvider.getTargetContainerCount(metaResourceName) < numContainerMax) {
    	  int newCount = targetProvider.getTargetContainerCount(metaResourceName) + numContainerStep;
    	  
          log.info(String.format("Increasing container count to %d", newCount));
	      targetProvider.setTargetContainerCount(metaResourceName, newCount);
	      
	      triggerPipeline(admin);	      
	      waitUntilRebalancedCount(newCount, admin);
	      printStep(String.format("Increased container count to %d", newCount), admin);
      }
      
      log.info("Destroying container 0 and container 1");
	  int currentCount = targetProvider.getTargetContainerCount(metaResourceName);
      providers.get(0).destroy("container_0");
      providers.get(0).destroy("container_1");
      triggerPipeline(admin);
      waitUntilRebalancedCount(currentCount, admin);
      printStep("Destroyed container 0 and container 1", admin);
      
      log.info("Destroying container provider 0");
	  currentCount = targetProvider.getTargetContainerCount(metaResourceName);
      managerProcesses[0].stop();
      waitUntilRebalancedCount(currentCount, admin);
      printStep("Destroyed container provider 0", admin);
      
      while(targetProvider.getTargetContainerCount(metaResourceName) > numContainerMin) {
    	  int newCount = targetProvider.getTargetContainerCount(metaResourceName) - numContainerStep;
    	  
          log.info(String.format("Decreasing container count to %d", newCount));
	      targetProvider.setTargetContainerCount(metaResourceName, newCount);
	      
	      triggerPipeline(admin);
	      waitUntilRebalancedCount(newCount, admin);
	      printStep(String.format("Decreased container count to %d", targetProvider.getTargetContainerCount(metaResourceName)), admin);
      }
      
      log.info("Stopping processes");
      
    } catch (Exception e)
    {
      e.printStackTrace();
    } finally
    {
      if (managedControllerManager != null) {
	      log.info("Disconnecting managed cluster controller");
    	  managedControllerManager.disconnect();
      }
      if (metaControllerManager != null) {
	      log.info("Disconnecting meta cluster controller");
        metaControllerManager.disconnect();
      }
	  log.info("Destroying meta processes");
      for (ProviderProcess process : managerProcesses) {
    	  process.stop();
      }
    }
    
    // TODO clean up threads correctly
    System.exit(0);
  }

private static void triggerPipeline(HelixAdmin admin) {
	IdealState poke = admin.getResourceIdealState(metaClusterName, metaResourceName);
	  admin.setResourceIdealState(metaClusterName, metaResourceName, poke);
}
  
  private static void printStep(String text, HelixAdmin admin) throws Exception {
	  log.info("********************************************************************************");
      log.info(text);
      log.info("********************************************************************************");
      printClusterStatus(admin);
      
      System.out.println("Press ENTER to continue");
      System.in.read();
  }
  
  static void printClusterStatus(HelixAdmin admin) throws Exception {
      log.info("Managed cluster status");
      printStatusMasterSlave(admin);
      log.info("Meta cluster status");
      printMetaClusterStatus(admin);
  }
  
  static void waitUntilRebalancedCount(int containerCount, HelixAdmin admin) throws InterruptedException {
	  Thread.sleep(TIMESTEP_INTERVAL);
	  while(containerCount != getMetaContainerCount(admin) ||
			containerCount != getManagedContainerCount(admin)) {
		  Thread.sleep(TIMESTEP_INTERVAL);
  	  }
	  ClusterStateVerifier.verifyByPolling(new BestPossAndExtViewZkVerifier(zkAddress, managedClusterName));
  }

  static int getMetaContainerCount(HelixAdmin admin) {
	    Set<String> assignedInstances = new HashSet<String>();
		  
	    ExternalView externalView = admin.getResourceExternalView(metaClusterName, metaResourceName);
		  
	    for (String partitionName : externalView.getPartitionSet())
	    {
	      Map<String, String> stateMap = externalView.getStateMap(partitionName);
	      if(stateMap == null)
	  	    continue;
	    
	      for(String instanceName : stateMap.keySet()){
	        if ("ONLINE".equals(stateMap.get(instanceName))) {
	          assignedInstances.add(partitionName);
	          break;
	        }
	      }
	    }
	  
	    return assignedInstances.size();
	  }

  static int getManagedContainerCount(HelixAdmin admin) {
    Set<String> assignedInstances = new HashSet<String>();
	  
    ExternalView externalView = admin.getResourceExternalView(managedClusterName, managedResourceName);
	  
    for (String partitionName : externalView.getPartitionSet())
    {
      Map<String, String> stateMap = externalView.getStateMap(partitionName);
      if(stateMap == null)
  	    continue;
    
      for(String instanceName : stateMap.keySet()){
        if ("MASTER".equals(stateMap.get(instanceName)) ||
      	    "SLAVE".equals(stateMap.get(instanceName))) {
          assignedInstances.add(instanceName);
        }
      }
    }
  
    return assignedInstances.size();
  }

  static void printMetaClusterStatus(HelixAdmin admin)
  {
    ExternalView externalView = admin
        .getResourceExternalView(metaClusterName, metaResourceName);
    TreeSet<String> treeSet = new TreeSet<String>(
        externalView.getPartitionSet());
    log.info("container" + "\t" + "acquired by");
    log.info("======================================");
    for (String partitionName : treeSet)
    {
      Map<String, String> stateMap = externalView.getStateMap(partitionName);
      String acquiredBy = null;
      if (stateMap != null)
      {
        for(String instanceName:stateMap.keySet()){
          if ("ONLINE".equals(stateMap.get(instanceName))){
            acquiredBy = instanceName;
            break;
          }
        }
      }
      log.info(partitionName + "\t"
          + ((acquiredBy != null) ? acquiredBy : "NONE"));
    }
  }

  static void printStatusMasterSlave(HelixAdmin admin)
	  {
	    ExternalView externalView = admin
	        .getResourceExternalView(managedClusterName, managedResourceName);
	    TreeSet<String> treeSet = new TreeSet<String>(
	        externalView.getPartitionSet());
	    log.info("partition" + "\t" + "master" + "\t\t" + "slave");
	    log.info("============================================================");
	    for (String partitionName : treeSet)
	    {
	      Map<String, String> stateMap = externalView.getStateMap(partitionName);
	      String master = "NONE";
	      String slave = "NONE";
	      if (stateMap != null)
	      {
	        for(String instanceName:stateMap.keySet()){
	          if ("MASTER".equals(stateMap.get(instanceName))){
	        	  master = instanceName;
	          }
	          if ("SLAVE".equals(stateMap.get(instanceName))){
	        	  slave = instanceName;
	          }
	        }
	      }
	      log.info(String.format("%s\t%s\t%s", partitionName, master, slave));
	    }
	  }

  public static void startLocalZookeeper() throws Exception
  {
    ZkServer server = null;
	String baseDir = "/tmp/metamanager/";
	final String dataDir = baseDir + "zk/dataDir";
	final String logDir = baseDir + "zk/logDir";
    FileUtils.deleteDirectory(new File(dataDir));
    FileUtils.deleteDirectory(new File(logDir));

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient)
      {

      }
    };
    server = new ZkServer(dataDir, logDir, defaultNameSpace, zkPort);
    server.start();

  }
  
  private static ContainerProviderService createContainerProvider(String type) throws Exception {
      String providerName = "provider_" + providerCount;
      providerCount++;
      
	  if(PROVIDER_LOCAL.equalsIgnoreCase(type)) {
		  log.info("Using VM-local container provider");
		  LocalContainerProvider provider =  new LocalContainerProvider(zkAddress, managedClusterName, providerName);
		  provider.registerType("container", ContainerUtils.getPropertiesFromResource("container.properties"));
		  providers.add(provider);
		  return provider;
	  } else if (PROVIDER_SHELL.equalsIgnoreCase(type)) {
		  log.info("Using shell-based container provider");
		  ShellContainerProvider provider = new ShellContainerProvider(zkAddress, managedClusterName, providerName);
          provider.registerType("container", ContainerUtils.getPropertiesFromResource("container.properties"));
		  providers.add(provider);
		  return provider;
	  } else if (PROVIDER_YARN.equalsIgnoreCase(type)) {
	      YarnApplicationProperties properties = new YarnApplicationProperties();
	      properties.put(YarnApplicationProperties.HELIX_CLUSTER, managedClusterName);
	      properties.put(YarnApplicationProperties.HELIX_ZOOKEEPER, zkAddress);
	      properties.put(YarnApplicationProperties.PROVIDER_METADATA, zkAddress);
	      properties.put(YarnApplicationProperties.PROVIDER_NAME, providerName);
		  
		  log.info("Using yarn-based container provider");
		  YarnContainerProvider yarnProvider = new YarnContainerProvider(properties);
          yarnProvider.registerType("container", ContainerUtils.getPropertiesFromResource("container.properties"));
		  
		  providers.add(yarnProvider);
		  return yarnProvider;
	  } else {
		  throw new IllegalArgumentException(String.format("Unknown container provider type '%s'", type));
	  }
  }
  
  private static StatusProviderService createContainerStatusProvider(String type) throws Exception {
	  if(PROVIDER_LOCAL.equalsIgnoreCase(type)) {
		  log.info("Using VM-local container status provider");
		  return new LocalStatusProvider();
	  } else if (PROVIDER_SHELL.equalsIgnoreCase(type)) {
		  log.info("Using shell-based container status provider");
		  return new ShellStatusProvider();
	  } else if (PROVIDER_YARN.equalsIgnoreCase(type)) {
		  log.info("Using yarn-based container status provider");
		  return new YarnStatusProvider(zkAddress);
	  } else {
		  throw new IllegalArgumentException(String.format("Unknown container status provider type '%s'", type));
	  }
  }
  
  private static <T extends Service> T startService(T service) throws Exception {
      service.start();
      services.add(service);
      return service;
  }
}