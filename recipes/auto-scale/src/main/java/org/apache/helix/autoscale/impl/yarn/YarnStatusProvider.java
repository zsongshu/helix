package org.apache.helix.autoscale.impl.yarn;

import java.util.Properties;

import org.apache.helix.autoscale.StatusProviderService;
import org.apache.helix.autoscale.impl.yarn.YarnContainerData.ContainerState;
import org.apache.log4j.Logger;

/**
 * StatusProvider for YARN-based containers spawned via
 * {@link YarnContainerProvider}. Reads {@link YarnDataProvider} meta data.
 * Runnable and configurable service.
 * 
 */
public class YarnStatusProvider implements StatusProviderService {

    static final Logger       log = Logger.getLogger(YarnStatusProvider.class);

    String                    yarndata;

    ZookeeperYarnDataProvider yarnDataService;

    public YarnStatusProvider() {
        // left blank
    }

    public YarnStatusProvider(String yarndata) {
        this.yarndata = yarndata;
        this.yarnDataService = new ZookeeperYarnDataProvider(yarndata);
    }

    @Override
    public void configure(Properties properties) throws Exception {
        this.yarndata = properties.getProperty("yarndata");
        this.yarnDataService = new ZookeeperYarnDataProvider(yarndata);
    }

    @Override
    public void start() throws Exception {
        yarnDataService = new ZookeeperYarnDataProvider(yarndata);
        yarnDataService.start();
    }

    @Override
    public void stop() throws Exception {
        if (yarnDataService != null) {
            yarnDataService.stop();
            yarnDataService = null;
        }
    }

    @Override
    public boolean exists(String id) {
        return yarnDataService.exists(id);
    }

    @Override
    public boolean isHealthy(String id) {
        try {
            return yarnDataService.read(id).state == ContainerState.ACTIVE;
        } catch (Exception e) {
            log.warn(String.format("Could not get activity data of %s", id));
            return false;
        }
    }

}
