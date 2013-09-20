package org.apache.helix.autoscale.impl.yarn;

import org.apache.helix.autoscale.container.ContainerProcessProperties;

import com.google.common.base.Preconditions;

/**
 * Base configuration for {@link YarnContainerProcess}. 
 *
 */
public class YarnContainerProcessProperties extends ContainerProcessProperties {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2209509977839674160L;
	
	public final static String YARNDATA = "yarndata";
	
	public boolean isValid() {
		return super.isValid() &&
		       containsKey(YARNDATA);
	}
	
	public String getYarnData() {
		return getProperty(YARNDATA);
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
