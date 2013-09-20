package org.apache.helix.metamanager.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.helix.metamanager.TargetProviderService;

/**
 * File-based target model. Container count is extracted from properties file. Count may change dynamically.
 * 
 */
public class FileTargetProvider implements TargetProviderService {

    File file;

    public FileTargetProvider() {
        // left blank
    }

    public FileTargetProvider(String path) {
        this.file = new File(path);
    }

    @Override
    public int getTargetContainerCount(String containerType) throws FileNotFoundException, IOException, IllegalArgumentException {
        Properties properties = new Properties();
        properties.load(new FileReader(file));
        if (!properties.contains(containerType))
            throw new IllegalArgumentException(String.format("container type '%s' not found in '%s'", containerType, file.getCanonicalPath()));
        return Integer.parseInt((String) properties.get(containerType));
    }

    @Override
    public void configure(Properties properties) throws Exception {
        this.file = new File(properties.getProperty("path"));
    }

    @Override
    public void start() throws Exception {
        // left blank
    }

    @Override
    public void stop() throws Exception {
        // left blank
    }

}
