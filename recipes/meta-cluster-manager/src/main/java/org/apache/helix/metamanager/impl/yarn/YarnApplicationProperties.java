package org.apache.helix.metamanager.impl.yarn;

import java.util.Properties;

import org.apache.helix.metamanager.container.ContainerProcessProperties;

import com.google.common.base.Preconditions;

public class YarnApplicationProperties extends Properties {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2209509977839674160L;
	
	public final static String HELIX_ZOOKEEPER = ContainerProcessProperties.HELIX_ZOOKEEPER;
	public final static String HELIX_CLUSTER = ContainerProcessProperties.HELIX_CLUSTER;
	public final static String PROVIDER_METADATA = "provider.metadata";
	public final static String PROVIDER_NAME = "provider.name";
	public final static String CONTAINER_ID = "container.id";
    public final static String YARN_RESOURCEMANAGER = "yarn.resourcemananger";
    public final static String YARN_SCHEDULER = "yarn.scheduler";
    public final static String YARN_USER = "yarn.user";
    public final static String YARN_HDFS= "yarn.hdfs";

	public boolean isValidMaster() {
		return containsKey(HELIX_ZOOKEEPER) &&
			   containsKey(HELIX_CLUSTER) &&
			   containsKey(PROVIDER_METADATA) &&
			   containsKey(PROVIDER_NAME) &&
			   containsKey(YARN_RESOURCEMANAGER) &&
			   containsKey(YARN_SCHEDULER) &&
			   containsKey(YARN_USER) &&
			   containsKey(YARN_HDFS);
	}
	
	public boolean isValidContainer() {
		return containsKey(HELIX_ZOOKEEPER) &&
			   containsKey(HELIX_CLUSTER) &&
			   containsKey(PROVIDER_METADATA) &&
			   containsKey(CONTAINER_ID);
	}
	
	public String getHelixZookeeper() {
		return getProperty(HELIX_ZOOKEEPER);
	}

	public String getHelixCluster() {
		return getProperty(HELIX_CLUSTER);
	}

	public String getProviderMetadata() {
		return getProperty(PROVIDER_METADATA);
	}

	public String getProviderName() {
		return getProperty(PROVIDER_NAME);
	}
	
	public String getContainerId() {
		return getProperty(CONTAINER_ID);
	}
	
    public String getYarnResourceManager() {
        return getProperty(YARN_RESOURCEMANAGER);
    }

    public String getYarnScheduler() {
        return getProperty(YARN_SCHEDULER);
    }

    public String getYarnUser() {
        return getProperty(YARN_USER);
    }

    public String getYarnHdfs() {
        return getProperty(YARN_HDFS);
    }

    @Override
    public Object get(Object key) {
        Preconditions.checkState(containsKey(key));
        return super.get(key);
    }
    
    @Override
    public String getProperty(String key) {
        Preconditions.checkState(containsKey(key));
        return super.getProperty(key);
    }
    
}
