package org.apache.helix.autoscale.container;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;

import org.apache.log4j.Logger;

/**
 * Utility for loading ContainerProperties and spawning ContainerProcess.
 * 
 */
public class ContainerUtils {

    static final Logger log = Logger.getLogger(ContainerUtils.class);

    private ContainerUtils() {
        // left blank
    }

    public static ContainerProcess createProcess(ContainerProcessProperties properties) throws Exception {
        String containerClassName = properties.getContainerClass();

        Class<?> containerClass = Class.forName(containerClassName);

        log.debug(String.format("checking for properties constructor in class '%s'", containerClassName));

        Constructor<?> constructor = containerClass.getConstructor(ContainerProcessProperties.class);

        return (ContainerProcess) constructor.newInstance(properties);
    }

    public static ContainerProcessProperties getPropertiesFromResource(String resourceName) throws IOException {
        ContainerProcessProperties properties = new ContainerProcessProperties();
        properties.load(ClassLoader.getSystemResourceAsStream(resourceName));
        return properties;
    }

    public static ContainerProcessProperties getPropertiesFromPath(String filePath) throws IOException {
        ContainerProcessProperties properties = new ContainerProcessProperties();
        properties.load(new InputStreamReader(new FileInputStream(filePath)));
        return properties;
    }

}
