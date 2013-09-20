package org.apache.helix.autoscale.provider;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.helix.HelixManager;
import org.apache.helix.autoscale.StatusProvider;
import org.apache.helix.autoscale.TargetProvider;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Rebalancer for meta cluster. Polls {@link TargetProvider} and
 * {@link StatusProvider} and reads and sets IdealState of meta cluster participants (
 * {@link ProviderProcess}). The number of active container is set to the target
 * count. Failed containers are shut down and restarted on any available
 * provider. Also, container counts are balanced across multiple providers.<br/>
 * <b>NOTE:</b> status and target provider are injected via
 * {@link ProviderRebalancerSingleton}<br/>
 * <br/>
 * <b>IdealState mapping:</b><br/>
 * resource = container type<br/>
 * partition = logical container instance<br/>
 * instance = container provider<br/>
 * status = physical container instance presence<br/>
 */
public class ProviderRebalancer implements Rebalancer {

    static final Logger log                 = Logger.getLogger(ProviderRebalancer.class);

    static final long   UPDATE_INTERVAL_MIN = 1500;

    static final Object lock                = new Object();
    static long         nextUpdate          = 0;

    TargetProvider      targetProvider;
    StatusProvider      statusProvider;
    HelixManager        manager;

    @Override
    public void init(HelixManager manager) {
        this.targetProvider = ProviderRebalancerSingleton.getTargetProvider();
        this.statusProvider = ProviderRebalancerSingleton.getStatusProvider();
        this.manager = manager;
    }

    @Override
    public ResourceAssignment computeResourceMapping(Resource resource, IdealState idealState, CurrentStateOutput currentStateOutput,
            ClusterDataCache clusterData) {

        final String resourceName = resource.getResourceName();
        final String containerType = resourceName;

        final SortedSet<String> allContainers = Sets.newTreeSet(new IndexedNameComparator());
        allContainers.addAll(idealState.getPartitionSet());

        final SortedSet<String> allProviders = Sets.newTreeSet(new IndexedNameComparator());
        for (LiveInstance instance : clusterData.getLiveInstances().values()) {
            allProviders.add(instance.getId());
        }

        final ResourceState currentState = new ResourceState(resourceName, currentStateOutput);

        // target container count
        log.debug(String.format("Retrieving target container count for type '%s'", containerType));
        int targetCount = -1;
        try {
            targetCount = targetProvider.getTargetContainerCount(containerType);
        } catch (Exception e) {
            log.error(String.format("Could not retrieve target count for '%s'", containerType), e);
            return new ResourceAssignment(resourceName);
        }

        // provider sanity check
        if (allProviders.isEmpty()) {
            log.warn(String.format("Could not find any providers"));
            return new ResourceAssignment(resourceName);
        }

        // all containers
        SortedSet<String> assignedContainers = getAssignedContainers(currentState, allContainers);
        SortedSet<String> failedContainers = getFailedContainers(currentState, allContainers);

        log.info(String.format("Rebalancing '%s' (target=%d, active=%d, failures=%d)", resourceName, targetCount, assignedContainers.size(),
                failedContainers.size()));

        if (log.isDebugEnabled()) {
            log.debug(String.format("%s: assigned containers %s", resourceName, assignedContainers));
            log.debug(String.format("%s: failed containers %s", resourceName, failedContainers));
        }

        // assignment
        int maxCountPerProvider = (int) Math.ceil(targetCount / (float) allProviders.size());

        ResourceAssignment assignment = new ResourceAssignment(resourceName);
        CountMap counts = new CountMap(allProviders);
        int assignmentCount = 0;

        // currently assigned
        for (String containerName : assignedContainers) {
            String providerName = getProvider(currentState, containerName);
            Partition partition = new Partition(containerName);

            if (failedContainers.contains(containerName)) {
                log.warn(String.format("Container '%s:%s' failed, going offline", providerName, containerName));
                assignment.addReplicaMap(partition, Collections.singletonMap(providerName, "OFFLINE"));

            } else if (counts.get(providerName) >= maxCountPerProvider) {
                log.warn(String.format("Container '%s:%s' misassigned, going offline", providerName, containerName));
                assignment.addReplicaMap(partition, Collections.singletonMap(providerName, "OFFLINE"));

            } else {
                assignment.addReplicaMap(partition, Collections.singletonMap(providerName, "ONLINE"));
            }

            counts.increment(providerName);
            assignmentCount++;
        }

        // currently unassigned
        SortedSet<String> unassignedContainers = Sets.newTreeSet(new IndexedNameComparator());
        unassignedContainers.addAll(allContainers);
        unassignedContainers.removeAll(assignedContainers);

        for (String containerName : unassignedContainers) {
            if (assignmentCount >= targetCount)
                break;

            String providerName = counts.getMinKey();
            Partition partition = new Partition(containerName);

            if (failedContainers.contains(containerName)) {
                log.warn(String.format("Container '%s:%s' failed and unassigned, going offline", providerName, containerName));
                assignment.addReplicaMap(partition, Collections.singletonMap(providerName, "OFFLINE"));

            } else {
                assignment.addReplicaMap(partition, Collections.singletonMap(providerName, "ONLINE"));
            }

            counts.increment(providerName);
            assignmentCount++;
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("assignment counts: %s", counts));
            log.debug(String.format("assignment: %s", assignment));
        }

        return assignment;
    }

    boolean hasProvider(ResourceState state, String containerName) {
        Map<String, String> currentStateMap = state.getCurrentStateMap(containerName);
        Map<String, String> pendingStateMap = state.getPendingStateMap(containerName);
        return hasInstance(currentStateMap, "ONLINE") || hasInstance(pendingStateMap, "ONLINE");
    }

    String getProvider(ResourceState state, String containerName) {
        Map<String, String> currentStateMap = state.getCurrentStateMap(containerName);
        if (hasInstance(currentStateMap, "ONLINE"))
            return getInstance(currentStateMap, "ONLINE");

        Map<String, String> pendingStateMap = state.getPendingStateMap(containerName);
        return getInstance(pendingStateMap, "ONLINE");
    }

    SortedSet<String> getFailedContainers(ResourceState state, Collection<String> containers) {
        SortedSet<String> failedContainers = Sets.newTreeSet(new IndexedNameComparator());
        for (String containerName : containers) {
            Map<String, String> currentStateMap = state.getCurrentStateMap(containerName);
            Map<String, String> pendingStateMap = state.getPendingStateMap(containerName);

            if (hasInstance(currentStateMap, "ERROR")) {
                failedContainers.add(containerName);
                continue;
            }

            if (!hasInstance(currentStateMap, "ONLINE") || hasInstance(pendingStateMap, "OFFLINE"))
                continue;

            // container listed online and not in transition, but not active
            if (!statusProvider.isHealthy(containerName)) {
                log.warn(String.format("Container '%s' designated ONLINE, but is not active", containerName));
                failedContainers.add(containerName);
            }
        }
        return failedContainers;
    }

    SortedSet<String> getAssignedContainers(ResourceState state, Collection<String> containers) {
        SortedSet<String> assignedContainers = Sets.newTreeSet(new IndexedNameComparator());
        for (String containerName : containers) {

            if (!hasProvider(state, containerName))
                continue;

            assignedContainers.add(containerName);
        }
        return assignedContainers;
    }

    boolean hasInstance(Map<String, String> stateMap, String state) {
        if (!stateMap.isEmpty()) {
            for (Map.Entry<String, String> entry : stateMap.entrySet()) {
                if (entry.getValue().equals(state)) {
                    return true;
                }
            }
        }
        return false;
    }

    String getInstance(Map<String, String> stateMap, String state) {
        if (!stateMap.isEmpty()) {
            for (Map.Entry<String, String> entry : stateMap.entrySet()) {
                if (entry.getValue().equals(state)) {
                    return entry.getKey();
                }
            }
        }
        throw new IllegalArgumentException(String.format("Could not find instance with state '%s'", state));
    }

    class IndexedNameComparator implements Comparator<String> {
        Pattern pattern = Pattern.compile("^(.*)([0-9]+)$");

        @Override
        public int compare(String o1, String o2) {
            Matcher m1 = pattern.matcher(o1);
            Matcher m2 = pattern.matcher(o2);

            boolean find1 = m1.find();
            boolean find2 = m2.find();

            if (!find1 && !find2)
                return o1.compareTo(o2);

            if (!find1 && find2)
                return -1;

            if (find1 && !find2)
                return 1;

            String name1 = m1.group(1);
            String name2 = m2.group(1);

            int name_comp = name1.compareTo(name2);
            if (name_comp != 0)
                return name_comp;

            int index1 = Integer.valueOf(m1.group(2));
            int index2 = Integer.valueOf(m2.group(2));

            return (int) Math.signum(index1 - index2);
        }
    }

    class CountMap extends HashMap<String, Integer> {
        /**
         * 
         */
        private static final long serialVersionUID = 3954138748385337978L;

        public CountMap(Collection<String> keys) {
            super();
            for (String key : keys) {
                put(key, 0);
            }
        }

        @Override
        public Integer get(Object key) {
            Preconditions.checkArgument(containsKey(key), "Key %s not found", key);
            return super.get(key);
        }

        public int increment(String key) {
            int newValue = get(key) + 1;
            Preconditions.checkArgument(containsKey(key), "Key %s not found", key);
            put(key, newValue);
            return newValue;
        }

        public String getMinKey() {
            Preconditions.checkState(size() > 0, "Must contain at least one item");

            String minKey = null;
            int minValue = Integer.MAX_VALUE;

            for (String key : keySet()) {
                int value = get(key);
                if (value < minValue) {
                    minValue = value;
                    minKey = key;
                }
            }

            return minKey;
        }

        public String getMaxKey() {
            Preconditions.checkState(size() > 0, "Must contain at least one item");

            String maxKey = null;
            int maxValue = Integer.MIN_VALUE;

            for (String key : keySet()) {
                int value = get(key);
                if (value > maxValue) {
                    maxValue = value;
                    maxKey = key;
                }
            }

            return maxKey;
        }
    }

    class ResourceState {
        final String             resourceName;
        final CurrentStateOutput state;

        public ResourceState(String resourceName, CurrentStateOutput state) {
            this.resourceName = resourceName;
            this.state = state;
        }

        Map<String, String> getCurrentStateMap(String partitionName) {
            return state.getCurrentStateMap(resourceName, new Partition(partitionName));
        }

        Map<String, String> getPendingStateMap(String partitionName) {
            return state.getPendingStateMap(resourceName, new Partition(partitionName));
        }
    }
}