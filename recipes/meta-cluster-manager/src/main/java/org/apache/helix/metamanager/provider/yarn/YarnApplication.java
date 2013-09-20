package org.apache.helix.metamanager.provider.yarn;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

public class YarnApplication {

	static final Logger log = Logger.getLogger(YarnApplication.class);
	
	static final String ENV_CLUSTER_ADDRESS = "YA_CLUSTER_ADDRESS";
	static final String ENV_CLUSTER_NAME = "YA_CLUSTER_NAME";
	static final String ENV_METADATA_ADDRESS = "YA_METADATA_ADDRESS";
	static final String ENV_PROVIDER_NAME = "YA_PROVIDER_NAME";

	static final String MASTER_COMMAND = "/bin/sh /home/apucher/incubator-helix/recipes/meta-cluster-manager/target/meta-cluster-manager-pkg/bin/yarn-master-process.sh 1>%s/stdout 2>%s/stderr";

	Configuration conf;
	YarnRPC rpc;
	ClientRMProtocol rmClient;
	ApplicationId appId;
	
	final ApplicationConfig appConfig;

	public YarnApplication(ApplicationConfig appConfig) {
		this.appConfig = appConfig;
		configure(new YarnConfiguration());
	}

	public void start() throws Exception {
		connect();
		
		String command = String.format(MASTER_COMMAND, "/tmp/" + appConfig.providerName, "/tmp/" + appConfig.providerName); 
				//ApplicationConstants.LOG_DIR_EXPANSION_VAR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);

		log.info(String.format("Starting application '%s' provider '%s' (masterCommand='%s')", appConfig.metadataAddress, appConfig.providerName, command));

		// app id
		GetNewApplicationRequest appRequest = Records.newRecord(GetNewApplicationRequest.class);
		GetNewApplicationResponse appResponse = rmClient.getNewApplication(appRequest);

		this.appId = appResponse.getApplicationId();

		log.info(String.format("Acquired app id '%s' for '%s'", appId.toString(), appConfig.providerName));
		
		// command
		ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);
		launchContext.setCommands(Collections.singletonList(command));

		// resource limit
		Resource resource = Records.newRecord(Resource.class);
		resource.setMemory(256); // TODO make dynamic
		launchContext.setResource(resource);
		
	    // environment
	    Map<String, String> env = new HashMap<String, String>();
	    env.put(ENV_CLUSTER_ADDRESS, appConfig.clusterAddress);
	    env.put(ENV_CLUSTER_NAME, appConfig.clusterName);
	    env.put(ENV_METADATA_ADDRESS, appConfig.metadataAddress);
	    env.put(ENV_PROVIDER_NAME, appConfig.providerName);
	    launchContext.setEnvironment(env);
	    
	    // local resources
	    // YARN workaround: create dummy resource 
	    Map<String, LocalResource> localResources = Utils.getDummyResources();
	    launchContext.setLocalResources(localResources);
	    
	    // app submission
	    ApplicationSubmissionContext subContext = Records.newRecord(ApplicationSubmissionContext.class);
		subContext.setApplicationId(appId);
		subContext.setApplicationName(appConfig.providerName);
		subContext.setAMContainerSpec(launchContext);

		SubmitApplicationRequest subRequest = Records.newRecord(SubmitApplicationRequest.class);
		subRequest.setApplicationSubmissionContext(subContext);
		
		log.info(String.format("Starting app id '%s'", appId.toString()));

		rmClient.submitApplication(subRequest);
		
	}

	public void stop() throws YarnRemoteException {
		log.info(String.format("Stopping app id '%s'", appId.toString()));
		KillApplicationRequest killRequest = Records.newRecord(KillApplicationRequest.class);
		killRequest.setApplicationId(appId);

		rmClient.forceKillApplication(killRequest);
	}

	void configure(Configuration conf) {
		this.conf = Preconditions.checkNotNull(conf);
		this.rpc = YarnRPC.create(conf);
	}

	void connect() {
		YarnConfiguration yarnConf = new YarnConfiguration(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
				YarnConfiguration.RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_ADDRESS));
		log.info("Connecting to ResourceManager at: " + rmAddress);
		this.rmClient = ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, conf));
	}
}
