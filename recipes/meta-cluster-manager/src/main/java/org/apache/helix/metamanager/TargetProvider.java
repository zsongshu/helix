package org.apache.helix.metamanager;

import org.apache.helix.metamanager.provider.ProviderRebalancer;

/**
 * Abstraction for target computation and statistics collection. Provides target
 * count of containers for ProviderRebalancer. Is polled by ProviderRebalancer
 * and should be light-weight and non-blocking.<br/>
 * <b>NOTE:</b> The target count is oblivious of failed containers and can be
 * obtained in an arbitrary way. See implementations for examples.
 * 
 * @see ProviderRebalancer
 */
public interface TargetProvider {

    /**
     * Return target count of containers of a specific type.
     * 
     * @param containerType
     *            meta resource name
     * @return container count >= 1
     * @throws Exception
     */
    public int getTargetContainerCount(String containerType) throws Exception;
}
