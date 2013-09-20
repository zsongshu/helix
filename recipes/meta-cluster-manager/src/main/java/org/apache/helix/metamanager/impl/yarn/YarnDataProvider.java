package org.apache.helix.metamanager.impl.yarn;

import java.util.Collection;

/**
 * Abstraction for a (remote) repository of yarn container meta data. Meta data
 * is read and updated by {@link YarnContainerProvider}
 * {@link YarnMasterProcess}, {@link YarnContainerProcess}.<br/>
 * <b>NOTE:</b> Each operation is assumed to be atomic.
 * 
 */
interface YarnDataProvider {

    /**
     * Checks for existence of meta data about container insatnce
     * 
     * @param id
     *            unique container id
     * @return true, if meta data exists
     */
    public boolean exists(String id);

    /**
     * Create meta data entry. Check for non-existence of meta data for given
     * container id and create node.
     * 
     * @param data
     *            container meta data with unique id
     * @throws Exception
     *             if meta data entry already exist
     */
    public void create(YarnContainerData data) throws Exception;

    /**
     * Read meta data for given container id.
     * 
     * @param id
     *            unique container id
     * @return yarn container data
     * @throws Exception
     *             if meta data entry for given id does not exist
     */
    public YarnContainerData read(String id) throws Exception;

    /**
     * Read all meta data stored for this domain space of yarn providers and
     * containers.
     * 
     * @return collection of meta data entries, empty if none
     * @throws Exception
     */
    public Collection<YarnContainerData> readAll() throws Exception;

    /**
     * Write meta data entry.
     * 
     * @param data
     *            yarn container meta data
     * @throws Exception
     *             if meta data entry for given id does not exist
     */
    public void update(YarnContainerData data) throws Exception;

    /**
     * Delete meta data entry. Frees up unique id to be reused. May throw an
     * exception on non-existence or be idempotent.
     * 
     * @param id
     *            unique container id
     * @throws Exception
     */
    public void delete(String id) throws Exception;
}