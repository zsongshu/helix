package org.apache.helix.metamanager;

public interface ClusterContainerProvider {
	/**
	 * Create container of given type.
	 * 
	 * @param id
	 *            unique user-defined container id
	 * @param type
	 *            container type
	 * @throws Exception
	 * @return connection string
	 */
	public void create(String id, String type) throws Exception;

	/**
	 * Destroy container.
	 * 
	 * @param id
	 *            unique user-defined container id
	 * @return connection string
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
