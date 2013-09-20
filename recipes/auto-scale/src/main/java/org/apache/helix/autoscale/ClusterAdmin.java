package org.apache.helix.autoscale;

/**
 * Abstraction for instance config (container) injection into and removal from
 * the managed cluster.
 * 
 */
public interface ClusterAdmin {

    /**
     * Add instance configuration to managed cluster.
     * 
     * @param instanceId
     * @param instanceTag
     */
    public void addInstance(String instanceId, String instanceTag);

    /**
     * Remove instance configuration from managed cluster.<br/>
     * <b>INVARIANT:</b> idempotent
     * 
     * @param instanceId
     */
    public void removeInstance(String instanceId);

    /**
     * Trigger rebalance of any affected resource in the managed cluster.
     */
    public void rebalance();
}
