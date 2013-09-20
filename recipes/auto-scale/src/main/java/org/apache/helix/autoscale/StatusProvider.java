package org.apache.helix.autoscale;

import org.apache.helix.autoscale.provider.ProviderRebalancer;

/**
 * Abstraction for status reader of container deployment framework. Provides
 * information on physical existence of container and activity or failure state.
 * Is polled by ProviderRebalancer and should be light-weight and non-blocking.<br/>
 * <b>NOTE:</b> This information is solely based on the low-level framework and
 * may be different from the participant state in Helix. (The Helix participant
 * may not even exist)
 * 
 * @see ProviderRebalancer
 */
public interface StatusProvider {

    /**
     * Determine whether container physically exists.
     * 
     * @param id
     *            unique container id
     * @return true, if container is present
     */
    public boolean exists(String id);

    /**
     * Determine whether container is healthy as determined by the deployment
     * framework.
     * 
     * @param id
     *            unique container id
     * @return true, if container is healthy
     */
    public boolean isHealthy(String id);
}
