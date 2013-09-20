package org.apache.helix.metamanager.integration;

import java.util.Collections;

import org.apache.helix.metamanager.TestUtils;
import org.apache.helix.metamanager.impl.StaticTargetProvider;
import org.apache.helix.metamanager.impl.local.LocalContainerProviderProcess;
import org.apache.helix.metamanager.impl.local.LocalStatusProvider;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LocalContainerProviderIT {
	
	static final Logger log = Logger.getLogger(LocalContainerProviderIT.class);
	
	static final int CONTAINER_COUNT = 4;

	StaticTargetProvider clusterStatusProvider;
	LocalContainerProviderProcess containerProvider;
	LocalStatusProvider containerStatusProvider;
	
	@BeforeMethod
	public void setupTest() throws Exception {
		teardownTest();
		TestUtils.startZookeeper();
		containerProvider = TestUtils.makeLocalProvider("provider_0");
		clusterStatusProvider = new StaticTargetProvider(Collections.singletonMap(TestUtils.metaResourceName, CONTAINER_COUNT));
		containerStatusProvider = new LocalStatusProvider();
		TestUtils.startTestCluster(clusterStatusProvider, containerStatusProvider, containerProvider);
	}
	
	@AfterMethod
	public void teardownTest() throws Exception {
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
