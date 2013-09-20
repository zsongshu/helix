package org.apache.helix.metamanager.bootstrap;

import java.util.Map;
import java.util.Properties;

import org.apache.helix.metamanager.container.ContainerProcessProperties;
import org.apache.log4j.Logger;

public class BootUtil {

    public static final String CLASS_PROPERTY = "class";
    static final Logger log = Logger.getLogger(BootUtil.class);
    
    public static Properties getNamespace(String namespace, Properties source) {
        Properties dest = new Properties();
        String prefix = namespace + ".";
        
        for(Map.Entry<Object, Object> rawEntry : source.entrySet()) {
            String key = (String)rawEntry.getKey();
            String value = (String)rawEntry.getValue();
            
            if(key.startsWith(prefix)) {
                String newKey = key.substring(prefix.length());
                dest.put(newKey, value);
            }
        }
        
        return dest;
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T createInstance(Properties properties) throws Exception {
        String className = properties.getProperty(CLASS_PROPERTY);
        
        Class<?> containerClass = Class.forName(className);
        
        try {
            log.debug(String.format("checking for properties constructor in class '%s'", className));
            return (T)containerClass.getConstructor(ContainerProcessProperties.class).newInstance(properties);
        } catch (Exception e) {
            log.debug("no properties constructor found");
        }
        
        try {
            log.debug(String.format("checking for default constructor in class '%s'", className));
            return (T)containerClass.getConstructor().newInstance();
        } catch (Exception e) {
            log.debug("no default constructor found");
        }
        
        throw new Exception(String.format("no suitable constructor for class '%s'", className));
    }
    
    private BootUtil() {
        // left blank
    }
    
}
