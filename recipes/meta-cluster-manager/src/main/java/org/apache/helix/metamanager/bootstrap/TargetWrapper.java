package org.apache.helix.metamanager.bootstrap;

import java.util.Properties;

import org.apache.helix.metamanager.TargetProvider;
import org.apache.helix.metamanager.TargetProviderService;
import org.apache.helix.metamanager.impl.FileTargetProvider;
import org.apache.helix.metamanager.impl.RedisTargetProvider;
import org.apache.helix.metamanager.impl.StaticTargetProvider;
import org.apache.log4j.Logger;

public class TargetWrapper {

    static final Logger   log = Logger.getLogger(TargetWrapper.class);

    WrapperImpl           impl;
    Properties            properties;
    TargetProviderService target;

    public TargetWrapper(Properties properties) {
        this.properties = properties;
    }

    public void startService() throws Exception {
        String type = (String) properties.get("type");

        log.info(String.format("starting target service (type='%s')", type));

        if ("static".equals(type)) {
            impl = new StaticWrapperImpl();

        } else if ("file".equals(type)) {
            impl = new FileWrapperImpl();

        } else if ("redis".equals(type)) {
            impl = new RedisWrapperImpl();

        } else {
            throw new IllegalArgumentException(String.format("type '%s' not supported", type));
        }

        impl.startService();
    }

    public void stopService() throws Exception {
        log.info("stopping target service");
        impl.stopService();
        target = null;
    }

    public TargetProvider getTarget() {
        return target;
    }

    static interface WrapperImpl {
        void startService() throws Exception;

        void stopService() throws Exception;
    }

    private class StaticWrapperImpl implements WrapperImpl {
        @Override
        public void startService() throws Exception {
            log.debug("creating static target provider");
            Properties prop = new Properties();
            prop.putAll(properties);
            prop.remove("type");

            target = new StaticTargetProvider();
            target.configure(prop);
            target.start();
        }

        @Override
        public void stopService() throws Exception {
            target.stop();
        }
    }

    private class FileWrapperImpl implements WrapperImpl {
        @Override
        public void startService() throws Exception {
            log.debug("creating file target provider");
            Properties prop = new Properties();
            prop.putAll(properties);
            prop.remove("type");

            target = new FileTargetProvider();
            target.configure(prop);
            target.start();
        }

        @Override
        public void stopService() throws Exception {
            target.stop();
        }
    }

    private class RedisWrapperImpl implements WrapperImpl {
        @Override
        public void startService() throws Exception {
            log.debug("creating redis target provider");
            Properties prop = new Properties();
            prop.putAll(properties);
            prop.remove("type");

            target = new RedisTargetProvider();
            target.configure(prop);
            target.start();
        }

        @Override
        public void stopService() throws Exception {
            target.stop();
        }
    }
}
