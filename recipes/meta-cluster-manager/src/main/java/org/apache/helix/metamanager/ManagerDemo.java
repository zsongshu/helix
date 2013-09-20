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
import org.apache.helix.metamanager.managed.HelixClusterAdmin;
import org.apache.helix.metamanager.managed.LocalStatusProvider;
import org.apache.helix.metamanager.provider.local.LocalContainerProvider;
import org.apache.helix.metamanager.provider.local.LocalContainerStatusProvider;
import org.apache.helix.metamanager.provider.shell.ShellContainerProvider;
import org.apache.helix.metamanager.provider.shell.ShellContainerStatusProvider;
import org.apache.helix.metamanager.provider.yarn.ApplicationConfig;
import org.apache.helix.metamanager.provider.yarn.YarnApplication;
import org.apache.helix.metamanager.provider.yarn.YarnContainerProvider;
import org.apache.helix.metamanager.provider.yarn.YarnContainerStatusProvider;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

public class ManagerDemo
{
	static final long TIMESTEP_INTERVAL = 1000;
	
	static final String MANAGED_PROCESS_PATH = "target/meta-cluster-manager-pkg/bin/container-process.sh";
	static final String YARN_PROCESS_PATH    = "/home/apucher/incubator-helix/recipes/meta-cluster-manager/target/meta-cluster-manager-pkg/bin/yarn-container-process.sh";

	static final String PROVIDER_LOCAL = "LOCAL";
	static final String PROVIDER_SHELL = "SHELL";
	static final String PROVIDER_YARN  = "YARN";
	
	static final Logger log = Logger.getLogger(ManagerDemo.class);
	
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
	
	static List<ClusterContainerProvider> providers = new ArrayList<ClusterContainerProvider>();
	static int providerCount = 0;
	    
	static Collection<YarnContainerProvider> yarnProviders = new ArrayList<YarnContainerProvider>();
	static Collection<YarnContainerStatusProvider> yarnStatusProviders = new ArrayList<YarnContainerStatusProvider>();
	static Collection<YarnApplication> yarnApplications = new ArrayList<YarnApplication>();
	
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
	  
    LocalStatusProvider clusterStatusProvider = null;
    ManagerProcess[] managerProcesses = new ManagerProcess[numContainerProviders];

    HelixManager metaControllerManager = null;
    HelixManager managedControllerManager = null;
    
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
		@Override
		public void run() {
		      for(ClusterContainerProvider provider : providers) {
			      log.info("Destroying all containers of provider");
			      provider.destroyAll();
		      }
		      for(YarnContainerProvider provider : yarnProviders) {
			      log.info("Stopping yarn container provider");
			      provider.stopService();   
		      }
		      for(YarnContainerStatusProvider provider : yarnStatusProviders) {
			      log.info("Stopping yarn container status provider");
			      provider.stopService();   
		      }
			  for(YarnApplication application: yarnApplications) {
				  log.info("Stopping yarn application");
				  try { application.stop(); } catch(Exception ignore) {}
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
      clusterStatusProvider = new LocalStatusProvider(numContainerMin);
      ClusterContainerStatusProvider containerStatusProvider = createContainerStatusProvider(containerProviderType); 
      
      log.info("Setup config tool");
      ConfigTool.setClusterStatusProvider(clusterStatusProvider);
      ConfigTool.setContainerStatusProvider(containerStatusProvider);
      
      // Managed Cluster
      log.info("Setup managed cluster");
      admin.addStateModelDef(managedClusterName, "MasterSlave",
          new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave()));
      admin.addResource(managedClusterName, managedResourceName, numManagedPartitions,
          "MasterSlave", IdealStateModeProperty.AUTO_REBALANCE.toString());
      admin.rebalance(managedClusterName, managedResourceName, numManagedReplica);
      
      // Meta Cluster
      log.info("Setup meta cluster");
      admin.addStateModelDef(metaClusterName, "OnlineOffline",
          new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));
      admin.addResource(metaClusterName, metaResourceName, clusterStatusProvider.getTargetContainerCount(""),
          "OnlineOffline", IdealStateModeProperty.AUTO_REBALANCE.toString());
      
      IdealState idealState = admin.getResourceIdealState(metaClusterName, metaResourceName);
      idealState.setRebalancerClassName(ManagerRebalancer.class.getName());
      //idealState.getRecord().setSimpleField(IdealStateProperty.REBALANCE_TIMER_PERIOD.toString(), "2000"); // Timer trigger creates race condition
      admin.setResourceIdealState(metaClusterName, metaResourceName, idealState);	  
      admin.rebalance(metaClusterName, metaResourceName, 1);
      
      log.info("Starting meta processes (container providers)");
      for (int i = 0; i < numContainerProviders; i++)
      {
        String instanceName = "provider_" + i;
        admin.addInstance(metaClusterName, new InstanceConfig(instanceName));
        
        ClusterAdmin clusterAdmin = new HelixClusterAdmin(managedClusterName, managedResourceName, numManagedReplica, admin);

        managerProcesses[i] = new ManagerProcess(metaClusterName, zkAddress,
                instanceName, createContainerProvider(containerProviderType), clusterAdmin);
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
      
      while(clusterStatusProvider.getTargetContainerCount("") < numContainerMax) {
    	  int newCount = clusterStatusProvider.getTargetContainerCount("") + numContainerStep;
    	  
          log.info(String.format("Increasing container count to %d", newCount));
	      clusterStatusProvider.setTargetContainerCount(newCount);
	      
	      triggerPipeline(admin);	      
	      waitUntilRebalancedCount(newCount, admin);
	      printStep(String.format("Increased container count to %d", newCount), admin);
      }
      
      log.info("Destroying container 0 and container 1");
	    int currentCount = clusterStatusProvider.getTargetContainerCount("");
      providers.get(0).destroy("container_0");
      providers.get(0).destroy("container_1");
      triggerPipeline(admin);
      waitUntilRebalancedCount(currentCount, admin);
      printStep("Destroyed container 0 and container 1", admin);
      
      log.info("Destroying container provider 0");
	  currentCount = clusterStatusProvider.getTargetContainerCount("");
      managerProcesses[0].stop();
      waitUntilRebalancedCount(currentCount, admin);
      printStep("Destroyed container provider 0", admin);
      
      while(clusterStatusProvider.getTargetContainerCount("") > numContainerMin) {
    	  int newCount = clusterStatusProvider.getTargetContainerCount("") - numContainerStep;
    	  
          log.info(String.format("Decreasing container count to %d", newCount));
	      clusterStatusProvider.setTargetContainerCount(newCount);
	      
	      triggerPipeline(admin);
	      waitUntilRebalancedCount(newCount, admin);
	      printStep(String.format("Decreased container count to %d", clusterStatusProvider.getTargetContainerCount("")), admin);
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
      for (ManagerProcess process : managerProcesses) {
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
    String baseDir = "/tmp/IntegrationTest/";
    final String dataDir = baseDir + "zk/dataDir";
    final String logDir = baseDir + "/tmp/logDir";
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
  
  private static ClusterContainerProvider createContainerProvider(String type) throws Exception {
      String providerName = "provider_" + providerCount;
      providerCount++;
      
	  if(PROVIDER_LOCAL.equalsIgnoreCase(type)) {
		  log.info("Using VM-local container provider");
		  LocalContainerProvider provider =  new LocalContainerProvider(zkAddress, managedClusterName, providerName);
		  providers.add(provider);
		  return provider;
	  } else if (PROVIDER_SHELL.equalsIgnoreCase(type)) {
		  log.info("Using shell-based container provider");
		  ShellContainerProvider provider = new ShellContainerProvider(zkAddress, managedClusterName, providerName, MANAGED_PROCESS_PATH);
		  providers.add(provider);
		  return provider;
	  } else if (PROVIDER_YARN.equalsIgnoreCase(type)) {
	      ApplicationConfig appConfig = new ApplicationConfig(zkAddress, managedClusterName, zkAddress, providerName);
		  
		  log.info("Using yarn-based container provider");
		  YarnApplication yarnApplication = new YarnApplication(appConfig);
		  yarnApplication.start();
		  yarnApplications.add(yarnApplication);
		  
		  YarnContainerProvider yarnProvider = new YarnContainerProvider(appConfig, YARN_PROCESS_PATH);
		  yarnProvider.startService();
		  yarnProviders.add(yarnProvider);
		  
		  providers.add(yarnProvider);
		  return yarnProvider;
	  } else {
		  throw new IllegalArgumentException(String.format("Unknown container provider type '%s'", type));
	  }
  }
  
  private static ClusterContainerStatusProvider createContainerStatusProvider(String type) throws Exception {
	  if(PROVIDER_LOCAL.equalsIgnoreCase(type)) {
		  log.info("Using VM-local container status provider");
		  LocalContainerStatusProvider provider = new LocalContainerStatusProvider();
		  return provider;
	  } else if (PROVIDER_SHELL.equalsIgnoreCase(type)) {
		  log.info("Using shell-based container status provider");
		  ShellContainerStatusProvider provider = new ShellContainerStatusProvider();
		  return provider;
	  } else if (PROVIDER_YARN.equalsIgnoreCase(type)) {
		  log.info("Using yarn-based container status provider");
		  YarnContainerStatusProvider provider = new YarnContainerStatusProvider(zkAddress);
		  provider.startService();
		  yarnStatusProviders.add(provider);
		  return provider;
	  } else {
		  throw new IllegalArgumentException(String.format("Unknown container status provider type '%s'", type));
	  }
  }
}