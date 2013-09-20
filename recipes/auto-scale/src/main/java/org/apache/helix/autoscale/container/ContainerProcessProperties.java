package org.apache.helix.autoscale.container;

import java.util.Properties;

import com.google.common.base.Preconditions;

/**
 * Base configuration for ContainerProcess. 
 * 
 */
public class ContainerProcessProperties extends Properties {
    /**
	 * 
	 */
    private static final long  serialVersionUID = 5754863079470995536L;

    public static final String CLUSTER          = "cluster";
    public static final String ADDRESS          = "address";
    public static final String NAME             = "name";
    public static final String CONTAINER_CLASS  = "class";

    public ContainerProcessProperties() {
        // left blank
    }

    public ContainerProcessProperties(Properties properties) {
        Preconditions.checkNotNull(properties);
        putAll(properties);
    }
	
	public boolean isValid() {
		return containsKey(CLUSTER) &&
			   containsKey(NAME) &&
			   containsKey(ADDRESS) &&
			   containsKey(CONTAINER_CLASS);
	}
	
    public String getCluster() {
        return getProperty(CLUSTER);
    }

    public String getAddress() {
        return getProperty(ADDRESS);
    }

    public String getName() {
        return getProperty(NAME);
    }

    public String getContainerClass() {
        return getProperty(CONTAINER_CLASS);
    }

    @Override
    public synchronized Object get(Object key) {
        Preconditions.checkState(containsKey(key));
        return super.get(key);
    }

    @Override
    public String getProperty(String key) {
        Preconditions.checkState(containsKey(key));
        return super.getProperty(key);
    }
	
}
