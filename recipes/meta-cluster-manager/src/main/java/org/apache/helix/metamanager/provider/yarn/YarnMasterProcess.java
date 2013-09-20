package org.apache.helix.metamanager.provider.yarn;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.log4j.Logger;

public class YarnMasterProcess {

	static final Logger log = Logger.getLogger(YarnMasterProcess.class);
	
	public static void main(String[] args) throws Exception {
		log.trace("BEGIN YarnMaster.main()");

		final ApplicationAttemptId appAttemptId = getApplicationAttemptId();
		log.info(String.format("Got application attempt id '%s'", appAttemptId.toString()));
		
		log.debug("Connecting to resource manager");
		Configuration conf = new YarnConfiguration();
		
		final AMRMProtocol resourceManager = getResourceManager(conf);

	    // register the AM with the RM
		log.debug("Registering application master");
	    RegisterApplicationMasterRequest appMasterRequest = 
	        Records.newRecord(RegisterApplicationMasterRequest.class);
	    appMasterRequest.setApplicationAttemptId(appAttemptId);     
	    appMasterRequest.setHost("");
	    appMasterRequest.setRpcPort(0);
	    appMasterRequest.setTrackingUrl("");

	    resourceManager.registerApplicationMaster(appMasterRequest);

	    String clusterAddress = getEnv(YarnApplication.ENV_CLUSTER_ADDRESS);
	    String clusterName = getEnv(YarnApplication.ENV_CLUSTER_NAME);
	    String metadataAddress = getEnv(YarnApplication.ENV_METADATA_ADDRESS);
	    String providerName = getEnv(YarnApplication.ENV_PROVIDER_NAME);
	    ApplicationConfig appConfig = new ApplicationConfig(clusterAddress, clusterName, metadataAddress, providerName);
	    
		log.debug("Launching metadata service");
	    final ZookeeperMetadataService metaService = new ZookeeperMetadataService(metadataAddress);
	    metaService.startService();
	    
		log.debug("Launching yarn master service");
	    final YarnMasterService service = new YarnMasterService(resourceManager, conf, appAttemptId, appConfig, metaService);
	    service.startService();
	    
		log.debug("Installing shutdown hooks");
	    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
	    	@Override
	    	public void run() {

	    		service.stopService();
	    		
	    		metaService.stopService();
	    		
	    		// finish application
	    	    log.debug("Sending finish request");
	    	    FinishApplicationMasterRequest finishReq = 
	    	    	Records.newRecord(FinishApplicationMasterRequest.class);
	    	    
	    	    finishReq.setAppAttemptId(getApplicationAttemptId());
	    	    finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
	    	    
	    	    try { resourceManager.finishApplicationMaster(finishReq); } catch(Exception ignore) {}
	    	}
	    }));
	    
	    System.out.println("Press ENTER to stop master service");
	    System.in.read();
	    
		log.trace("END YarnMaster.main()");
	}
	
	static AMRMProtocol getResourceManager(Configuration conf) {
		// Connect to the Scheduler of the ResourceManager.
	    YarnConfiguration yarnConf = new YarnConfiguration(conf);
	    YarnRPC rpc = YarnRPC.create(yarnConf);
	    InetSocketAddress rmAddress = 
	        NetUtils.createSocketAddr(yarnConf.get(
	            YarnConfiguration.RM_SCHEDULER_ADDRESS,
	            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
	    log.info("Connecting to ResourceManager at " + rmAddress);
	    AMRMProtocol resourceManager = 
	        (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf);
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
	      throw new IllegalArgumentException(
	          String.format("%s not set in the environment", key));
	    }
	    return clusterName;
	}

}
