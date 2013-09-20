package org.apache.helix.metamanager.impl.yarn;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.metamanager.ConfigTool;
import org.apache.helix.metamanager.Service;
import org.apache.log4j.Logger;

class YarnApplication implements Service {

	static final Logger log = Logger.getLogger(YarnApplication.class);
	
	static final String ENV_CLUSTER_ADDRESS  = "YA_CLUSTER_ADDRESS";
	static final String ENV_CLUSTER_NAME     = "YA_CLUSTER_NAME";
	static final String ENV_METADATA_ADDRESS = "YA_METADATA_ADDRESS";
	static final String ENV_PROVIDER_NAME    = "YA_PROVIDER_NAME";

	static String YARN_MASTER_COMMAND = "/bin/sh %s 1>%s/stdout 2>%s/stderr";
	
	Configuration conf;
	YarnRPC rpc;
	ClientRMProtocol rmClient;
	ApplicationId appId;
	File propertiesFile;
	
	YarnApplicationProperties properties;

	public YarnApplication() {
	    // left blank
	}
	
    public YarnApplication(YarnApplicationProperties properties) {
        this.properties = properties;
        internalConf();
    }
    
    @Override
    public void configure(Properties properties) throws Exception {
        YarnApplicationProperties yarnProps = new YarnApplicationProperties();
        yarnProps.putAll(properties);
        this.properties = yarnProps;
        internalConf();
    }

    public void internalConf() {
        this.conf = new YarnConfiguration();
        this.conf.set(YarnConfiguration.RM_ADDRESS, properties.getYarnResourceManager());
        this.conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, properties.getYarnScheduler());
        this.conf.set(FileSystem.FS_DEFAULT_NAME_KEY, properties.getYarnHdfs());

        this.rpc = YarnRPC.create(conf);
    }
    
    @Override
    public void start() throws Exception {
		connect();
		
		String command = String.format(YARN_MASTER_COMMAND, ConfigTool.YARN_MASTER_PATH,
				ApplicationConstants.LOG_DIR_EXPANSION_VAR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);

		log.info(String.format("Starting application '%s' provider '%s' (masterCommand='%s')",
				properties.getProviderMetadata(), properties.getProviderName(), command));

		log.debug(String.format("Running master command \"%s\"", command));
		
		// app id
		GetNewApplicationRequest appRequest = Records.newRecord(GetNewApplicationRequest.class);
		GetNewApplicationResponse appResponse = rmClient.getNewApplication(appRequest);

		this.appId = appResponse.getApplicationId();

		log.info(String.format("Acquired app id '%s' for '%s'", appId.toString(), properties.getProviderName()));
		
		// command
		ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);
		launchContext.setCommands(Collections.singletonList(command));

		// resource limit
		Resource resource = Records.newRecord(Resource.class);
		resource.setMemory(256); // TODO make dynamic
		launchContext.setResource(resource);
		
	    // environment
	    Map<String, String> env = new HashMap<String, String>();
	    launchContext.setEnvironment(env);
	    
	    // configuration
	    propertiesFile = YarnUtils.writePropertiesToTemp(properties);
	    
	    // HDFS
	    final String namespace = appId.toString();
	    final Path masterArchive = YarnUtils.copyToHdfs(ConfigTool.YARN_MASTER_ARCHIVE_PATH, ConfigTool.YARN_MASTER_STAGING, namespace, conf);
	    final Path masterProperties = YarnUtils.copyToHdfs(propertiesFile.getCanonicalPath(), ConfigTool.YARN_MASTER_PROPERTIES, namespace, conf);
	    final Path containerArchive = YarnUtils.copyToHdfs(ConfigTool.YARN_CONTAINER_ARCHIVE_PATH, ConfigTool.YARN_CONTAINER_STAGING, namespace, conf);
	    
	    // local resources
	    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
	    localResources.put(ConfigTool.YARN_MASTER_DESTINATION, 
	    		YarnUtils.createHdfsResource(masterArchive, LocalResourceType.ARCHIVE, conf));
	    localResources.put(ConfigTool.YARN_MASTER_PROPERTIES,
	    		YarnUtils.createHdfsResource(masterProperties, LocalResourceType.FILE, conf));
        localResources.put(ConfigTool.YARN_CONTAINER_STAGING,
                YarnUtils.createHdfsResource(containerArchive, LocalResourceType.FILE, conf));
	    
	    launchContext.setLocalResources(localResources);
	    
	    // user
	    launchContext.setUser(properties.getYarnUser());
	    
	    // app submission
	    ApplicationSubmissionContext subContext = Records.newRecord(ApplicationSubmissionContext.class);
		subContext.setApplicationId(appId);
		subContext.setApplicationName(properties.getProviderName());
		subContext.setAMContainerSpec(launchContext);

		SubmitApplicationRequest subRequest = Records.newRecord(SubmitApplicationRequest.class);
		subRequest.setApplicationSubmissionContext(subContext);
		
		log.info(String.format("Starting app id '%s'", appId.toString()));

		rmClient.submitApplication(subRequest);
		
	}

    @Override
	public void stop() throws YarnRemoteException {
		log.info(String.format("Stopping app id '%s'", appId.toString()));
		KillApplicationRequest killRequest = Records.newRecord(KillApplicationRequest.class);
		killRequest.setApplicationId(appId);

		rmClient.forceKillApplication(killRequest);

		try { YarnUtils.destroyHdfsNamespace(appId.toString(), conf); } catch(Exception ignore) {}
		
		propertiesFile.delete();
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
