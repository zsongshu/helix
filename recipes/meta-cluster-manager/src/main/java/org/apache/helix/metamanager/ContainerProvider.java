package org.apache.helix.metamanager;

import org.apache.helix.metamanager.provider.ProviderStateModel;

/**
 * Abstraction for container deployment framework. Creates and destroys
 * container instances. Is invoked by ProviderStateModel and must be blocking.
 * 
 * @see ProviderStateModel
 */
public interface ContainerProvider {
    /**
     * Create container of given type.<br/>
     * <b>INVARIANT:</b> synchronous invocation
     * 
     * @param id
     *            unique user-defined container id
     * @param containerType
     *            container type
     * @throws Exception
     */
    public void create(String id, String containerType) throws Exception;

    /**
     * Destroy container.<br/>
     * <b>INVARIANT:</b> synchronous invocation
     * 
     * @param id
     *            unique user-defined container id
     * @throws Exception
     */
    public void destroy(String id) throws Exception;

    /**
     * Stops all running processes and destroys containers. Best-effort for
     * cleanup.
     * 
     */
    public void destroyAll();
}
