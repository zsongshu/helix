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
import org.apache.helix.metamanager.Service;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Configurable and runnable service for {@link YarnContainerProvider}
 * 
 */
public class YarnContainerProviderProcess implements Service {

    static final Logger             log                 = Logger.getLogger(YarnContainerProviderProcess.class);

    static String                   YARN_MASTER_COMMAND = "/bin/sh %s 1>%s/stdout 2>%s/stderr";

    Configuration                   conf;
    YarnRPC                         rpc;
    ClientRMProtocol                rmClient;
    ApplicationId                   appId;
    File                            propertiesFile;

    YarnContainerProviderProperties properties;

    @Override
    public void configure(Properties properties) throws Exception {
        configure(YarnUtils.createContainerProviderProperties(properties));
    }

    private void configure(YarnContainerProviderProperties properties) {
        this.conf = new YarnConfiguration();
        this.conf.set(YarnConfiguration.RM_ADDRESS, properties.getResourceManager());
        this.conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, properties.getScheduler());
        this.conf.set(FileSystem.FS_DEFAULT_NAME_KEY, properties.getHdfs());

        this.rpc = YarnRPC.create(conf);

        this.properties = properties;
    }

    @Override
    public void start() throws Exception {
        Preconditions.checkNotNull(properties);
        Preconditions.checkState(properties.isValid());

        connect();

        String command = String.format(YARN_MASTER_COMMAND, YarnUtils.YARN_MASTER_PATH, ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                ApplicationConstants.LOG_DIR_EXPANSION_VAR);

        log.info(String.format("Starting application '%s' provider '%s' (masterCommand='%s')", properties.getYarnData(), properties.getName(), command));

        log.debug(String.format("Running master command \"%s\"", command));

        // app id
        GetNewApplicationRequest appRequest = Records.newRecord(GetNewApplicationRequest.class);
        GetNewApplicationResponse appResponse = rmClient.getNewApplication(appRequest);

        this.appId = appResponse.getApplicationId();

        log.info(String.format("Acquired app id '%s' for '%s'", appId.toString(), properties.getName()));

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
        final Path masterArchive = YarnUtils.copyToHdfs(YarnUtils.YARN_MASTER_ARCHIVE_PATH, YarnUtils.YARN_MASTER_STAGING, namespace, conf);
        final Path masterProperties = YarnUtils.copyToHdfs(propertiesFile.getCanonicalPath(), YarnUtils.YARN_MASTER_PROPERTIES, namespace, conf);
        final Path containerArchive = YarnUtils.copyToHdfs(YarnUtils.YARN_CONTAINER_ARCHIVE_PATH, YarnUtils.YARN_CONTAINER_STAGING, namespace, conf);

        // local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        localResources.put(YarnUtils.YARN_MASTER_DESTINATION, YarnUtils.createHdfsResource(masterArchive, LocalResourceType.ARCHIVE, conf));
        localResources.put(YarnUtils.YARN_MASTER_PROPERTIES, YarnUtils.createHdfsResource(masterProperties, LocalResourceType.FILE, conf));
        localResources.put(YarnUtils.YARN_CONTAINER_STAGING, YarnUtils.createHdfsResource(containerArchive, LocalResourceType.FILE, conf));

        launchContext.setLocalResources(localResources);

        // user
        launchContext.setUser(properties.getUser());

        // app submission
        ApplicationSubmissionContext subContext = Records.newRecord(ApplicationSubmissionContext.class);
        subContext.setApplicationId(appId);
        subContext.setApplicationName(properties.getName());
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
        InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS));
        log.info("Connecting to ResourceManager at: " + rmAddress);
        this.rmClient = ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, conf));
    }
}
