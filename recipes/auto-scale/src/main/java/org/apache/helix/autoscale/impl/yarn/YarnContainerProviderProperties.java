package org.apache.helix.autoscale.impl.yarn;

import org.apache.helix.autoscale.provider.ProviderProperties;

import com.google.common.base.Preconditions;

/**
 * Base configuration for {@link YarnContainerProviderProcess} 
 *
 */
public class YarnContainerProviderProperties extends ProviderProperties {
	/**
     * 
     */
    private static final long serialVersionUID = -8853614843205587170L;
    
	public final static String YARNDATA = "yarndata";
    public final static String RESOURCEMANAGER = "resourcemananger";
    public final static String SCHEDULER = "scheduler";
    public final static String USER = "user";
    public final static String HDFS = "hdfs";
    
	public boolean isValid() {
		return super.isValid() &&
		       containsKey(YARNDATA) &&
			   containsKey(RESOURCEMANAGER) &&
			   containsKey(SCHEDULER) &&
			   containsKey(USER) &&
			   containsKey(HDFS);
	}
	
	public String getYarnData() {
		return getProperty(YARNDATA);
	}

    public String getResourceManager() {
        return getProperty(RESOURCEMANAGER);
    }

    public String getScheduler() {
        return getProperty(SCHEDULER);
    }

    public String getUser() {
        return getProperty(USER);
    }

    public String getHdfs() {
        return getProperty(HDFS);
    }
    
    @Override
    public String getProperty(String key) {
        Preconditions.checkState(containsKey(key));
        return super.getProperty(key);
    }
    
    @Override
    public Object get(Object key) {
        Preconditions.checkState(containsKey(key));
        return super.get(key);
    }
    
}
