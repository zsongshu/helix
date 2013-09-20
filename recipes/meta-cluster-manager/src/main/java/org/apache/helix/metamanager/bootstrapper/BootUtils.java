package org.apache.helix.metamanager.bootstrapper;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Utility for instantiating bootstrapping services and parsing hierarchical
 * properties files.
 * 
 */
public class BootUtils {

    public static final String CLASS_PROPERTY = "class";
    static final Logger        log            = Logger.getLogger(BootUtils.class);

    public static boolean hasNamespace(Properties properties, String namespace) {
        String prefix = namespace + ".";
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith(prefix))
                return true;
        }
        return false;
    }

    public static Set<String> getNamespaces(Properties properties) {
        Pattern pattern = Pattern.compile("^([^\\.\\=]+)");

        Set<String> namespaces = Sets.newHashSet();

        for (Map.Entry<Object, Object> rawEntry : properties.entrySet()) {
            String key = (String) rawEntry.getKey();

            Matcher matcher = pattern.matcher(key);
            if (matcher.find()) {
                namespaces.add(matcher.group(1));
            }
        }

        return namespaces;
    }

    public static Properties getNamespace(Properties source, String namespace) {
        Properties dest = new Properties();
        String prefix = namespace + ".";

        for (Map.Entry<Object, Object> rawEntry : source.entrySet()) {
            String key = (String) rawEntry.getKey();
            String value = (String) rawEntry.getValue();

            if (key.startsWith(prefix)) {
                String newKey = key.substring(prefix.length());
                dest.put(newKey, value);
            }
        }

        return dest;
    }

    public static Collection<Properties> getContainerProps(Properties properties) {
        Collection<Properties> containerProps = Lists.newArrayList();

        String containers = properties.getProperty("containers");
        String containerTypes[] = StringUtils.split(containers, ",");

        for (String containerType : containerTypes) {
            Properties containerProp = BootUtils.getNamespace(BootUtils.getNamespace(properties, "container"), containerType);
            log.debug(String.format("adding container type (type='%s', properties='%s')", containerType, containerProp));
            containerProps.add(containerProp);
        }

        return containerProps;
    }

    @SuppressWarnings("unchecked")
    public static <T> T createInstance(Class<?> clazz) throws Exception {
        try {
            log.debug(String.format("checking for default constructor in class '%s'", clazz.getSimpleName()));
            return (T) clazz.getConstructor().newInstance();
        } catch (Exception e) {
            log.debug("no default constructor found");
        }

        throw new Exception(String.format("no suitable constructor for class '%s'", clazz.getSimpleName()));
    }

    public static <T> T createInstance(String className) throws Exception {
        return createInstance(Class.forName(className));
    }

    private BootUtils() {
        // left blank
    }

}
