package org.apache.helix.metamanager.bootstrap;

import java.util.Properties;

import org.apache.helix.metamanager.StatusProvider;
import org.apache.helix.metamanager.StatusProviderService;
import org.apache.helix.metamanager.impl.local.LocalStatusProvider;
import org.apache.helix.metamanager.impl.shell.ShellStatusProvider;
import org.apache.helix.metamanager.impl.yarn.YarnStatusProvider;
import org.apache.log4j.Logger;

public class StatusWrapper {

    static final Logger   log = Logger.getLogger(StatusWrapper.class);

    WrapperImpl           impl;
    StatusProviderService status;
    Properties            properties;

    public StatusWrapper(Properties properties) {
        this.properties = properties;
    }

    public void startService() throws Exception {
        String type = (String) properties.get("type");

        log.info(String.format("starting container status service (type='%s')", type));

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
        log.debug("stopping container status provider");
        impl.stopService();
        status = null;
    }

    public StatusProvider getStatus() {
        return status;
    }

    static interface WrapperImpl {
        void startService() throws Exception;

        void stopService() throws Exception;
    }

    class LocalWrapperImpl implements WrapperImpl {

        LocalStatusProvider status;

        @Override
        public void startService() throws Exception {
            log.debug("creating local container status provider");
            status = new LocalStatusProvider();
            status.configure(properties);
            status.start();

            StatusWrapper.this.status = status;
        }

        @Override
        public void stopService() throws Exception {
            status.stop();
        }
    }

    class ShellWrapperImpl implements WrapperImpl {

        ShellStatusProvider status;

        @Override
        public void startService() throws Exception {
            log.debug("creating shell container status provider");
            status = new ShellStatusProvider();
            status.configure(properties);
            status.start();
            StatusWrapper.this.status = status;
        }

        @Override
        public void stopService() throws Exception {
            status.stop();
        }
    }

    class YarnWrapperImpl implements WrapperImpl {

        YarnStatusProvider status;

        @Override
        public void startService() throws Exception {
            String metadata = properties.getProperty("metadata");

            log.debug(String.format("creating yarn container status provider (metadata='%s')", metadata));
            status = new YarnStatusProvider();
            status.configure(properties);
            status.start();

            StatusWrapper.this.status = status;
        }

        @Override
        public void stopService() throws Exception {
            status.stop();
        }
    }

}
