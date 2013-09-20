package org.apache.helix.autoscale.impl.yarn;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.autoscale.provider.ProviderProcess;
import org.apache.log4j.Logger;

/**
 * Host process for {@link YarnContainerProviderProcess}. Hasts application
 * master in YARN and provider participant to Helix meta cluster. (Program entry
 * point)
 * 
 */
class YarnMasterProcess {

    static final Logger log = Logger.getLogger(YarnMasterProcess.class);

    public static void main(String[] args) throws Exception {
        log.trace("BEGIN YarnMaster.main()");

        final ApplicationAttemptId appAttemptId = getApplicationAttemptId();
        log.info(String.format("Got application attempt id '%s'", appAttemptId.toString()));

        log.debug("Reading master properties");
        YarnMasterProperties properties = YarnUtils.createMasterProperties(YarnUtils.getPropertiesFromPath(YarnUtils.YARN_MASTER_PROPERTIES));

        if (!properties.isValid())
            throw new IllegalArgumentException(String.format("master properties not valid: %s", properties.toString()));

        log.debug("Connecting to resource manager");
        Configuration conf = new YarnConfiguration();
        conf.set(YarnConfiguration.RM_ADDRESS, properties.getResourceManager());
        conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, properties.getScheduler());
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, properties.getHdfs());

        final AMRMProtocol resourceManager = getResourceManager(conf);

        // register the AM with the RM
        log.debug("Registering application master");
        RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);
        appMasterRequest.setApplicationAttemptId(appAttemptId);
        appMasterRequest.setHost("");
        appMasterRequest.setRpcPort(0);
        appMasterRequest.setTrackingUrl("");

        resourceManager.registerApplicationMaster(appMasterRequest);

        log.debug("Starting yarndata service");
        final ZookeeperYarnDataProvider yarnDataService = new ZookeeperYarnDataProvider(properties.getYarnData());
        yarnDataService.start();

        log.debug("Starting yarn master service");
        final YarnMasterService service = new YarnMasterService();
        service.configure(properties);
        service.setAttemptId(appAttemptId);
        service.setYarnDataProvider(yarnDataService);
        service.setProtocol(resourceManager);
        service.setYarnConfiguration(conf);
        service.start();

        log.debug("Starting provider");
        final YarnContainerProvider provider = new YarnContainerProvider();
        provider.configure(properties);
        provider.start();

        log.debug("Starting provider process");
        final ProviderProcess process = new ProviderProcess();
        process.configure(properties);
        process.setConteinerProvider(provider);
        process.start();

        log.debug("Installing shutdown hooks");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.debug("Stopping provider process");
                process.stop();

                log.debug("Stopping provider");
                try { provider.stop(); } catch (Exception ignore) {}

                log.debug("Stopping yarn master service");
                service.stop();

                log.debug("Stopping yarndata service");
                yarnDataService.stop();

                // finish application
                log.debug("Sending finish request");
                FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);

                finishReq.setAppAttemptId(getApplicationAttemptId());
                finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
	    	    
	    	    try { resourceManager.finishApplicationMaster(finishReq); } catch(Exception ignore) {}
	    	}
	    }));
	    
		log.trace("END YarnMaster.main()");
	}
	
    static AMRMProtocol getResourceManager(Configuration conf) {
        // Connect to the Scheduler of the ResourceManager.
        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        YarnRPC rpc = YarnRPC.create(yarnConf);
        InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
        log.info("Connecting to ResourceManager at " + rmAddress);
        AMRMProtocol resourceManager = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf);
        return resourceManager;
    }

    static ApplicationAttemptId getApplicationAttemptId() {
        ContainerId containerId = ConverterUtils.toContainerId(getEnv(ApplicationConstants.AM_CONTAINER_ID_ENV));
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
        return appAttemptID;
    }

    static String getEnv(String key) {
        Map<String, String> envs = System.getenv();
        String clusterName = envs.get(key);
        if (clusterName == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException(String.format("%s not set in the environment", key));
        }
        return clusterName;
    }

}
