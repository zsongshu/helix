package org.apache.helix.metamanager.integration;

import java.util.Collections;

import org.apache.helix.metamanager.StaticStatusProvider;
import org.apache.helix.metamanager.TestUtils;
import org.apache.helix.metamanager.impl.local.LocalContainerProvider;
import org.apache.helix.metamanager.impl.local.LocalContainerSingleton;
import org.apache.helix.metamanager.impl.local.LocalContainerStatusProvider;
import org.apache.helix.metamanager.impl.shell.ShellContainerProvider;
import org.apache.helix.metamanager.impl.shell.ShellContainerSingleton;
import org.apache.helix.metamanager.impl.shell.ShellContainerStatusProvider;
import org.apache.helix.metamanager.impl.yarn.YarnContainerProvider;
import org.apache.helix.metamanager.impl.yarn.YarnContainerStatusProvider;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MultipleProviderFailoverIT {
	
	static final Logger log = Logger.getLogger(MultipleProviderFailoverIT.class);
	
	static final long TEST_TIMEOUT = 60000;
	static final long REBALANCE_TIMEOUT = 30000;

	static final int CONTAINER_COUNT = 7;

	StaticStatusProvider clusterStatusProvider;
	
	YarnContainerStatusProvider yarnStatusProvider;
	
	@BeforeClass
	public void setupClass() {
		log.info("installing shutdown hook");
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try { teardownTest(); } catch(Exception ignore) {};
			}
		}));
	}
	
	@BeforeMethod
	public void setupTest() throws Exception {
		teardownTest();
		TestUtils.startZookeeper();
		clusterStatusProvider = new StaticStatusProvider(Collections.singletonMap(TestUtils.metaResourceName, CONTAINER_COUNT));
	}
	
	@AfterMethod
	public void teardownTest() throws Exception {
		TestUtils.stopTestCluster();
		LocalContainerSingleton.reset();
		ShellContainerSingleton.reset();
		if(yarnStatusProvider != null) {
			yarnStatusProvider.stopService();
			yarnStatusProvider = null;
		}
		TestUtils.stopZookeeper();
	}
	
	@Test(timeOut = TEST_TIMEOUT)
	public void testLocalContainerFailover() throws Exception {
		log.info("testing local container failover");
		TestUtils.startTestCluster(clusterStatusProvider, new LocalContainerStatusProvider(), makeLocalProviders(3));
		killContainers();
	}
	
	@Test(timeOut = TEST_TIMEOUT)
	public void testLocalProviderFailover() throws Exception {
		log.info("testing local provider failover");
		TestUtils.startTestCluster(clusterStatusProvider, new LocalContainerStatusProvider(), makeLocalProviders(3));
		killProvider();
	}
	
	@Test(timeOut = TEST_TIMEOUT)
	public void testShellContainerFailover() throws Exception {
		log.info("testing shell container failover");
		TestUtils.startTestCluster(clusterStatusProvider, new ShellContainerStatusProvider(), makeShellProviders(3));
		killContainers();
	}
	
	@Test(timeOut = TEST_TIMEOUT)
	public void testShellProviderFailover() throws Exception {
		log.info("testing shell provider failover");
		TestUtils.startTestCluster(clusterStatusProvider, new ShellContainerStatusProvider(), makeShellProviders(3));
		killProvider();
	}
	
	@Test(timeOut = TEST_TIMEOUT)
	public void testYarnContainerFailover() throws Exception {
		log.info("testing yarn container failover");
		yarnStatusProvider = new YarnContainerStatusProvider(TestUtils.zkAddress);
		yarnStatusProvider.startService();
		TestUtils.startTestCluster(clusterStatusProvider, yarnStatusProvider, makeYarnProviders(3));
		killContainers();
	}
	
	@Test(timeOut = TEST_TIMEOUT)
	public void testYarnProviderFailover() throws Exception {
		log.info("testing yarn provider failover");
		yarnStatusProvider = new YarnContainerStatusProvider(TestUtils.zkAddress);
		yarnStatusProvider.startService();
		TestUtils.startTestCluster(clusterStatusProvider, yarnStatusProvider, makeYarnProviders(3));
		killProvider();
	}
	
	static void killContainers() throws Exception {
		TestUtils.containerProviders.get(1).destroy("container_2");
		TestUtils.containerProviders.get(1).destroy("container_4");
		TestUtils.containerProviders.get(1).destroy("container_6");
		TestUtils.rebalanceTestCluster();
		TestUtils.waitUntilRebalancedCount(CONTAINER_COUNT, REBALANCE_TIMEOUT);
	}
	
	static void killProvider() throws Exception {
		TestUtils.managerProcesses.get(1).stop();
		TestUtils.rebalanceTestCluster();
		TestUtils.waitUntilRebalancedCount(CONTAINER_COUNT, REBALANCE_TIMEOUT);
	}
	
	static LocalContainerProvider[] makeLocalProviders(int count) {
		LocalContainerProvider[] providers = new LocalContainerProvider[count];
		for(int i=0; i<count; i++) {
			providers[i] = TestUtils.makeLocalProvider("provider_" + i);
		}
		return providers;
	}
	
	static ShellContainerProvider[] makeShellProviders(int count) {
		ShellContainerProvider[] providers = new ShellContainerProvider[count];
		for(int i=0; i<count; i++) {
			providers[i] = TestUtils.makeShellProvider("provider_" + i);
		}
		return providers;
	}
	
	YarnContainerProvider[] makeYarnProviders(int count) throws Exception {
		YarnContainerProvider[] providers = new YarnContainerProvider[count];
		for(int i=0; i<count; i++) {
			providers[i] = TestUtils.makeYarnProvider("provider_" + i);
		}
		return providers;
	}
	
}
