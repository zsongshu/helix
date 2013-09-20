package org.apache.helix.metamanager.provider;

import org.apache.helix.metamanager.StatusProvider;
import org.apache.helix.metamanager.TargetProvider;
import org.apache.log4j.Logger;

/**
 * Utility for dependency injection into ProviderRebalancer.
 * 
 */
public class ProviderRebalancerSingleton {

    static final Logger   log = Logger.getLogger(ProviderRebalancerSingleton.class);

    static TargetProvider targetProvider;
    static StatusProvider statusProvider;

    private ProviderRebalancerSingleton() {
        // left blank
    }

    public static TargetProvider getTargetProvider() {
        return targetProvider;
    }

    public static void setTargetProvider(TargetProvider targetProvider) {
        ProviderRebalancerSingleton.targetProvider = targetProvider;
    }

    public static StatusProvider getStatusProvider() {
        return statusProvider;
    }

    public static void setStatusProvider(StatusProvider statusProvider) {
        ProviderRebalancerSingleton.statusProvider = statusProvider;
    }

}
