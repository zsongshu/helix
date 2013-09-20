package org.apache.helix.metamanager.integration;

import java.util.Collections;

import org.apache.helix.metamanager.TestUtils;
import org.apache.helix.metamanager.impl.StaticTargetProvider;
import org.apache.helix.metamanager.impl.yarn.YarnContainerProviderProcess;
import org.apache.helix.metamanager.impl.yarn.YarnContainerProviderProperties;
import org.apache.helix.metamanager.impl.yarn.YarnStatusProvider;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class YarnContainerProviderIT {
	
	static final Logger log = Logger.getLogger(YarnContainerProviderIT.class);
	
	static final int CONTAINER_COUNT = 4;

	StaticTargetProvider clusterStatusProvider;
	YarnContainerProviderProcess containerProvider;
	YarnStatusProvider containerStatusProvider;
	
	YarnContainerProviderProperties properties;
	
	@BeforeClass
	public void setupClass() throws Exception {
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
		log.debug("setting up yarn test case");
		
		teardownTest();
		TestUtils.startZookeeper();
		
		containerProvider = TestUtils.makeYarnProvider("provider_0");
		containerStatusProvider = new YarnStatusProvider(TestUtils.zkAddress);
		clusterStatusProvider = new StaticTargetProvider(Collections.singletonMap(TestUtils.metaResourceName, CONTAINER_COUNT));
		TestUtils.startTestCluster(clusterStatusProvider, containerStatusProvider, containerProvider);
		
		log.debug("running yarn test case");
	}
	
	@AfterMethod
	public void teardownTest() throws Exception {
		log.debug("cleaning up yarn test case");
		TestUtils.stopTestCluster();
		TestUtils.stopZookeeper();
	}
	
	@Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testStatic() throws Exception {
		log.info("testing static");
		setContainerCount(CONTAINER_COUNT);
	}
	
	@Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testScaleUp() throws Exception {
		log.info("testing scale up");
		setContainerCount(CONTAINER_COUNT + 2);
	}
	
	@Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testScaleDown() throws Exception {
		log.info("testing scale down");
		setContainerCount(CONTAINER_COUNT - 2);
	}
	
	@Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testScaleCycle() throws Exception {
		log.info("testing scale cycle");
		setContainerCount(CONTAINER_COUNT + 2);
		setContainerCount(CONTAINER_COUNT);
		setContainerCount(CONTAINER_COUNT - 2);
		setContainerCount(CONTAINER_COUNT);
	}
	
	void setContainerCount(int newContainerCount) throws Exception {
		log.debug(String.format("Setting container count to %d", newContainerCount));
		clusterStatusProvider.setTargetContainerCount(TestUtils.metaResourceName, newContainerCount);
		TestUtils.rebalanceTestCluster();
	}
}
