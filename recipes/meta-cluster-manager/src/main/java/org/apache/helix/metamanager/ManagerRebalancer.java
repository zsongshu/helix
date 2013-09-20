package org.apache.helix.metamanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.log4j.Logger;

/**
 * Rebalancer for cluster state. Uses cluster status provider.<br/>
 * <br/>
 * IdealState mapping:<br/>
 * resource     = tag-name<br/>
 *   partition  = logical container<br/>
 *     instance = resource provider<br/>
 *       status = physical container presence
 *
 */
public class ManagerRebalancer implements Rebalancer {
	
	static final Logger log = Logger.getLogger(ManagerRebalancer.class);
	
	static final long UPDATE_INTERVAL_MIN = 1500;
	
	static final Object lock = new Object();
	static long nextUpdate = 0; 

	ClusterStatusProvider clusterStatusProvider;
	ClusterContainerStatusProvider containerStatusProvider;
	HelixManager manager;
	
	@Override
	public void init(HelixManager manager) {
		this.clusterStatusProvider = ConfigTool.getClusterStatusProvider();
		this.containerStatusProvider = ConfigTool.getContainerStatusProvider();
		this.manager = manager;
	}

	@Override
	public IdealState computeNewIdealState(String resourceName,
			IdealState currentIdealState,
			CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
	
//		synchronized(lock) {
//			if(nextUpdate > System.currentTimeMillis()) {
//				return currentIdealState;
//			}
//			nextUpdate = System.currentTimeMillis() + UPDATE_INTERVAL_MIN;
		
			// target container count
			int targetCount = clusterStatusProvider.getTargetContainerCount(resourceName);
			
			// currently active containers
			List<String> currentPartitions = new ArrayList<String>();
			for(String partitionName : currentIdealState.getPartitionSet()) {
				Map<String, String> currentStateMap = currentStateOutput.getCurrentStateMap(resourceName, new Partition(partitionName));
				Map<String, String> pendingStateMap = currentStateOutput.getPendingStateMap(resourceName, new Partition(partitionName));
				
				if(hasOnlineInstance(currentStateMap) ||
				   hasOnlineInstance(pendingStateMap)) {
					currentPartitions.add(partitionName);
				}
			}
			int currentCount = currentPartitions.size();
			
			// currently failed containers
			List<String> failedPartitions = new ArrayList<String>();
			for(String partitionName : currentIdealState.getPartitionSet()) {
				Map<String, String> currentStateMap = currentStateOutput.getCurrentStateMap(resourceName, new Partition(partitionName));
				
				if(!hasOnlineInstance(currentStateMap))
					continue;
				
				// container listed online, but does not exist
				if(!containerStatusProvider.exists(partitionName)) {
					log.warn(String.format("Container '%s' designated ONLINE, but does not exist", partitionName));
					failedPartitions.add(partitionName);
				}
				
				// container listed online and exists, but in failure state
				if(containerStatusProvider.exists(partitionName) &&
				   containerStatusProvider.isFailed(partitionName)) {
					log.warn(String.format("Container '%s' designated ONLINE, but in failure state", partitionName));
					failedPartitions.add(partitionName);
				}
			}
			int failureCount = failedPartitions.size();
			
			if(currentCount != targetCount ||
			   failureCount != 0) {
				log.info(String.format("Rebalancing containers (current=%d, target=%d, failures=%d)", currentCount, targetCount, failureCount));
				
				currentIdealState.setNumPartitions(targetCount);
				
				// future active containers
				log.debug("active containers");
				List<String> activePartitions = new ArrayList<String>();
				for(int i=0; i<targetCount; i++) {
					String partitionName = resourceName + "_" + i;
					activePartitions.add(partitionName);
				}
				activePartitions.removeAll(failedPartitions);
				
				// future passive containers
				log.debug("passive containers");
				List<String> passivePartitions = new ArrayList<String>();
				for(int i=targetCount; i<currentCount; i++) {
					String partitionName = resourceName + "_" + i;
					passivePartitions.add(partitionName);
				}
				passivePartitions.addAll(failedPartitions);
				
				log.debug("output");
				if(log.isDebugEnabled()) {
					log.debug(String.format("%s: failed partitions %s", resourceName, failedPartitions));
					log.debug(String.format("%s: active partitions %s", resourceName, activePartitions));
					log.debug(String.format("%s: passive partitions %s", resourceName, passivePartitions));
				}
				
				log.debug("building ideal state");
				Map<String, List<String>> listFields = new HashMap<String, List<String>>();
				Map<String, Map<String, String>> mapFields = new HashMap<String, Map<String, String>>();
				for(String partitionName : activePartitions) {
					listFields.put(partitionName, new ArrayList<String>());
					mapFields.put(partitionName, new HashMap<String, String>());
				}
				currentIdealState.getRecord().setListFields(listFields);
				currentIdealState.getRecord().setMapFields(mapFields);
				
				log.debug("setting ideal state");
				String clusterName = manager.getClusterName();
				manager.getClusterManagmentTool().setResourceIdealState(clusterName, resourceName, currentIdealState);
				
				log.debug("enable partitions");
				for(String instanceName : clusterData.getInstanceConfigMap().keySet()) {
					log.debug(String.format("enable partitions for '%s'", instanceName));
					manager.getClusterManagmentTool().enablePartition(true, clusterName, instanceName, resourceName, activePartitions);
					log.debug(String.format("disable partitions for '%s'", instanceName));
					manager.getClusterManagmentTool().enablePartition(false, clusterName, instanceName, resourceName, passivePartitions);
				}
				
				log.debug("done");
			}
			
			return currentIdealState;
//		}
	}

	private boolean hasOnlineInstance(Map<String, String> stateMap) {
		if(!stateMap.isEmpty()) {
			for(Map.Entry<String, String> entry : stateMap.entrySet()) {
				if(entry.getValue().equals("ONLINE")) {
					return true;
				}
			}
		}
		return false;
	}
	
}
