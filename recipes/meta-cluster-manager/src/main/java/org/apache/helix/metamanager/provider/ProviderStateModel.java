package org.apache.helix.metamanager.provider;

import org.apache.helix.NotificationContext;
import org.apache.helix.metamanager.ClusterAdmin;
import org.apache.helix.metamanager.ContainerProvider;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

/**
 * Helix state model implementation for {@link ContainerProvider}s. Updates
 * configuration of managed Helix cluster and spawns and destroys container
 * instances.
 * 
 */
@StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "ONLINE" })
public class ProviderStateModel extends StateModel {

    static final Logger log = Logger.getLogger(ProviderStateModel.class);

    ContainerProvider   provider;
    ClusterAdmin        admin;

    public ProviderStateModel(ContainerProvider provider, ClusterAdmin admin) {
        this.provider = provider;
        this.admin = admin;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void acquire(Message m, NotificationContext context) throws Exception {
        String containerType = m.getResourceName();
        String containerId = m.getPartitionName();
        String instanceId = context.getManager().getInstanceName();

        log.trace(String.format("%s:%s transitioning from OFFLINE to ONLINE", containerId, instanceId));

        bestEffortRemove(containerId);

        // add instance to cluster
        admin.addInstance(containerId, containerType);

        // create container
        provider.create(containerId, containerType);

        try {
            admin.rebalance();
        } catch (Exception e) {
            // ignore
            log.warn(String.format("rebalancing cluster failed (error='%s')", e.getMessage()));
        }

        log.info(String.format("%s acquired container '%s' (type='%s')", instanceId, containerId, containerType));
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void release(Message m, NotificationContext context) {
        String containerId = m.getPartitionName();
        String instanceId = context.getManager().getInstanceName();

        log.trace(String.format("%s:%s transitioning from ONLINE to OFFLINE", containerId, instanceId));

        bestEffortRemove(containerId);

        try {
            admin.rebalance();
        } catch (Exception e) {
            // ignore
            log.warn(String.format("rebalancing cluster failed (error='%s')", e.getMessage()));
        }

        log.info(String.format("%s destroyed container '%s'", instanceId, containerId));

    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void recover(Message m, NotificationContext context) {
        String containerId = m.getPartitionName();
        String instanceId = context.getManager().getInstanceName();

        log.trace(String.format("%s:%s transitioning from ERROR to OFFLINE", containerId, instanceId));

        release(m, context);
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void drop(Message m, NotificationContext context) {
        String containerId = m.getPartitionName();
        String instanceId = context.getManager().getInstanceName();

        log.trace(String.format("%s:%s transitioning from OFFLINE to DROPPED", containerId, instanceId));
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
