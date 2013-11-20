package org.apache.helix.autoscale.impl.yarn;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
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
	
		log.debug("Reading master properties");
		YarnMasterProperties properties = YarnUtils
				.createMasterProperties(YarnUtils
						.getPropertiesFromPath(YarnUtils.YARN_MASTER_PROPERTIES));

		if (!properties.isValid())
			throw new IllegalArgumentException(String.format(
					"master properties not valid: %s", properties.toString()));


		log.debug("Starting yarndata service");
		final ZookeeperYarnDataProvider yarnDataService = new ZookeeperYarnDataProvider(
				properties.getYarnData());
		yarnDataService.start();

		log.debug("Starting yarn master service");
		final YarnMasterService service = new YarnMasterService();
		service.configure(properties);
		service.setYarnDataProvider(yarnDataService);
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
				try {
					provider.stop();
				} catch (Exception ignore) {
				}

				log.debug("Stopping yarn master service");
				service.stop();

				log.debug("Stopping yarndata service");
				yarnDataService.stop();
			}
		}));

		log.trace("END YarnMaster.main()");
	}

	static String getEnv(String key) {
		Map<String, String> envs = System.getenv();
		String clusterName = envs.get(key);
		if (clusterName == null) {
			// container id should always be set in the env by the framework
			throw new IllegalArgumentException(String.format(
					"%s not set in the environment", key));
		}
		return clusterName;
	}

}
