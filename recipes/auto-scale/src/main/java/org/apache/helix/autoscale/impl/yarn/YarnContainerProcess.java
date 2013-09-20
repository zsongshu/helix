package org.apache.helix.autoscale.impl.yarn;

import org.apache.helix.autoscale.container.ContainerProcess;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Host process for {@link ContainerProcess}es spawned by
 * {@link YarnContainerProvider}. Configured via *.properties file in working
 * directory. Corresponds to regular container in YARN and is managed jointly by
 * the application master and the Helix participant. (Program entry point)
 * 
 */
class YarnContainerProcess {
    static final Logger log = Logger.getLogger(YarnContainerProcess.class);

    public static void main(String[] args) throws Exception {
        log.trace("BEGIN YarnProcess.main()");

        final YarnContainerProcessProperties properties = YarnUtils.createContainerProcessProperties(YarnUtils
                .getPropertiesFromPath(YarnUtils.YARN_CONTAINER_PROPERTIES));
        Preconditions.checkArgument(properties.isValid(), "container properties not valid: %s", properties.toString());

        log.debug("Launching yarndata service");
        final ZookeeperYarnDataProvider metaService = new ZookeeperYarnDataProvider(properties.getYarnData());
        metaService.start();

        log.debug("Launching yarn container service");
        final YarnContainerService yarnService = new YarnContainerService();
        yarnService.configure(properties);
        yarnService.setYarnDataProvider(metaService);
        yarnService.start();

        log.debug("Installing shutdown hooks");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.debug("Running shutdown hook");
                yarnService.stop();
                metaService.stop();
            }
        }));

        System.out.println("Press ENTER to stop container process");
        System.in.read();

        log.debug("Stopping container services");
        System.exit(0);

        log.trace("END YarnProcess.main()");
    }
}
