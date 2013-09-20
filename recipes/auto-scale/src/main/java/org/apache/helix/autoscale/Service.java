package org.apache.helix.autoscale;

import java.util.Properties;

/**
 * Abstraction for configurable and runnable service. Light-weight dependency
 * injection and life-cycle management.
 * 
 */
public interface Service {

    /**
     * Configure service internals<br/>
     * <b>INVARIANT:</b> executed only once
     * 
     * @param properties
     *            arbitrary key-value properties, parsed internally
     * @throws Exception
     */
    void configure(Properties properties) throws Exception;

    /**
     * Start service.<br/>
     * <b>PRECONDITION:</b> configure() was invoked<br/>
     * <b>INVARIANT:</b> executed only once
     * 
     * @throws Exception
     */
    void start() throws Exception;

    /**
     * Stop service.<br/>
     * <b>INVARIANT:</b> idempotent
     * 
     * @throws Exception
     */
    void stop() throws Exception;
}
