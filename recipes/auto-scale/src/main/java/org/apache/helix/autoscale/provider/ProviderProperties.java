package org.apache.helix.autoscale.provider;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.helix.autoscale.bootstrapper.BootUtils;

import com.google.common.base.Preconditions;

/**
 * Base configuration for {@link ProviderProcess}. 
 *
 */
public class ProviderProperties extends Properties {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2209509977839674160L;
	
	public final static String ADDRESS = "address";
	public final static String CLUSTER = "cluster";
    public final static String METAADDRESS = "metaaddress";
    public final static String METACLUSTER = "metacluster";
	public final static String NAME = "name";
	
	public final static String CONTAINER_NAMESPACE = "containers";
	
	public boolean isValid() {
		return(containsKey(ADDRESS) &&
		       containsKey(CLUSTER) &&
		       containsKey(METAADDRESS) &&
               containsKey(METACLUSTER) &&
               containsKey(NAME));
	}
	
	public String getAddress() {
		return getProperty(ADDRESS);
	}
	
	public String getCluster() {
	    return getProperty(CLUSTER);
	}
	
    public String getMetaAddress() {
        return getProperty(METAADDRESS);
    }
    
    public String getMetaCluster() {
        return getProperty(METACLUSTER);
    }
    
	public String getName() {
	    return getProperty(NAME);
	}
	
	public Set<String> getContainers() {
        if(!BootUtils.hasNamespace(this, CONTAINER_NAMESPACE))
            return Collections.emptySet();
	    return BootUtils.getNamespaces(BootUtils.getNamespace(this, CONTAINER_NAMESPACE));
	}
	
	public boolean hasContainer(String id) {
	    if(!BootUtils.hasNamespace(this, CONTAINER_NAMESPACE)) return false;
	    if(!BootUtils.hasNamespace(BootUtils.getNamespace(this, CONTAINER_NAMESPACE), id)) return false;
	    return true;
	}
	
	public Properties getContainer(String id) {
	    Preconditions.checkArgument(BootUtils.hasNamespace(this, CONTAINER_NAMESPACE), "no container namespace");
        Preconditions.checkArgument(BootUtils.hasNamespace(BootUtils.getNamespace(this, CONTAINER_NAMESPACE), id), "container %s not configured", id);
	    return BootUtils.getNamespace(BootUtils.getNamespace(this, CONTAINER_NAMESPACE), id);
	}
	
	public void addContainer(String id, Properties properties) {
	    Preconditions.checkArgument(!getContainers().contains(id), "Already contains container type %s", id);
	    
	    // add container config
        for(Map.Entry<Object, Object> entry : properties.entrySet()) {
            this.put(CONTAINER_NAMESPACE + "." + id + "." + entry.getKey(), entry.getValue());
        }
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
