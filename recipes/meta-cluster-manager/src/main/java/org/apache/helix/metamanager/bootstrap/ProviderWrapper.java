package org.apache.helix.metamanager.bootstrap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.metamanager.impl.local.LocalContainerProviderProcess;
import org.apache.helix.metamanager.impl.shell.ShellContainerProviderProcess;
import org.apache.helix.metamanager.impl.yarn.YarnContainerProviderProcess;
import org.apache.helix.metamanager.impl.yarn.YarnContainerProviderProperties;
import org.apache.log4j.Logger;

public class ProviderWrapper {

    static final Logger      log = Logger.getLogger(ProviderWrapper.class);

    WrapperImpl              impl;
    Properties               properties;

    public ProviderWrapper(Properties properties) {
        this.properties = properties;
    }

    public void startService() throws Exception {
        String type = (String) properties.get("type");

        log.info(String.format("starting container provider service (type='%s')", type));

        if ("local".equals(type)) {
            impl = new LocalWrapperImpl();

        } else if ("shell".equals(type)) {
            impl = new ShellWrapperImpl();

        } else if ("yarn".equals(type)) {
            impl = new YarnWrapperImpl();

        } else {
            throw new IllegalArgumentException(String.format("type '%s' not supported", type));
        }

        impl.startService();
    }

    public void stopService() throws Exception {
        impl.stopService();
    }

    public String getProviderName() {
        return properties.getProperty("name");
    }

    public Set<String> getContainerTypes() {
        String containers = properties.getProperty("containers");
        String containerTypes[] = StringUtils.split(containers, ",");
        return new HashSet<String>(Arrays.asList(containerTypes));
    }

    static interface WrapperImpl {
        void startService() throws Exception;

        void stopService() throws Exception;
    }

    class LocalWrapperImpl implements WrapperImpl {
        LocalContainerProviderProcess process;

        @Override
        public void startService() throws Exception {
            String name = properties.getProperty("name");
            String address = properties.getProperty("address");
            String cluster = properties.getProperty("cluster");
            String containers = properties.getProperty("containers");

            log.debug(String.format("creating local container provider (name='%s', address='%s', cluster='%s', containers='%s')", name, address, cluster,
                    containers));

            process = new LocalContainerProviderProcess();
            process.configure(properties);
            process.start();
        }

        @Override
        public void stopService() throws Exception {
            process.stop();
            process = null;
        }

    }

    class ShellWrapperImpl implements WrapperImpl {

        ShellContainerProviderProcess process;

        @Override
        public void startService() throws Exception {
            String name = properties.getProperty("name");
            String address = properties.getProperty("address");
            String cluster = properties.getProperty("cluster");
            String containers = properties.getProperty("containers");

            log.debug(String.format("creating shell container provider (name='%s', address='%s', cluster='%s', containers='%s')", name, address, cluster,
                    containers));

            process = new ShellContainerProviderProcess();
            process.configure(properties);
            process.start();
        }

        @Override
        public void stopService() throws Exception {
            process.stop();
            process = null;
        }

    }

    class YarnWrapperImpl implements WrapperImpl {

        YarnContainerProviderProcess process;

        @Override
        public void startService() throws Exception {
            String name = properties.getProperty("name");
            String address = properties.getProperty("address");
            String cluster = properties.getProperty("cluster");
            String containers = properties.getProperty("containers");
            String metadata = properties.getProperty("metadata");
            String resourcemanager = properties.getProperty("resourcemananger");
            String scheduler = properties.getProperty("scheduler");
            String user = properties.getProperty("user");
            String hdfs = properties.getProperty("hdfs");

            YarnContainerProviderProperties yarnProperties = new YarnContainerProviderProperties();
            yarnProperties.setProperty(YarnContainerProviderProperties.CLUSTER, cluster);
            yarnProperties.setProperty(YarnContainerProviderProperties.ADDRESS, address);
            yarnProperties.setProperty(YarnContainerProviderProperties.NAME, name);
            yarnProperties.setProperty(YarnContainerProviderProperties.METADATA, metadata);
            yarnProperties.setProperty(YarnContainerProviderProperties.RESOURCEMANAGER, resourcemanager);
            yarnProperties.setProperty(YarnContainerProviderProperties.SCHEDULER, scheduler);
            yarnProperties.setProperty(YarnContainerProviderProperties.USER, user);
            yarnProperties.setProperty(YarnContainerProviderProperties.HDFS, hdfs);

            log.debug(String.format("creating yarn container provider (name='%s', address='%s', cluster='%s', metadata='%s', resourcemananger='%s', " +
            		"scheduler='%s', user='%s', hdfs='%s', containers='%s')", name, address, cluster, metadata, resourcemanager, scheduler, user, hdfs, containers));

            process = new YarnContainerProviderProcess();
            process.configure(yarnProperties);
            process.start();
        }

        @Override
        public void stopService() throws Exception {
            process.stop();
            process = null;
        }

    }

}
