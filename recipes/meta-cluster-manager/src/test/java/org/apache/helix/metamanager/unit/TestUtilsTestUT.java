package org.apache.helix.metamanager.unit;

import java.util.Collections;

import org.apache.helix.metamanager.ClusterStatusProvider;
import org.apache.helix.metamanager.ContainerProvider;
import org.apache.helix.metamanager.ContainerStatusProvider;
import org.apache.helix.metamanager.StaticStatusProvider;
import org.apache.helix.metamanager.TestUtils;
import org.apache.helix.metamanager.impl.local.LocalContainerSingleton;
import org.apache.helix.metamanager.impl.local.LocalContainerStatusProvider;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestUtilsTestUT {

	static final Logger log = Logger.getLogger(TestUtilsTestUT.class);
	
	@AfterMethod
	public void teardownTest() throws Exception {
		LocalContainerSingleton.reset();
	}
	
	@Test
	public void testZookeeper() throws Exception {
		log.info("testing zookeeper");
		TestUtils.startZookeeper();
		TestUtils.stopZookeeper();
	}

	@Test
	public void testCluster() throws Exception {
		log.info("testing cluster");
		TestUtils.startZookeeper();
		
		TestUtils.startTestCluster(new StaticStatusProvider(Collections.singletonMap(TestUtils.metaResourceName, 1)),
				new LocalContainerStatusProvider(), TestUtils.makeLocalProvider("test"));
		TestUtils.stopTestCluster();
		
		TestUtils.stopZookeeper();
	}

	@Test
	public void testClusterRepeated() throws Exception {
		log.info("testing cluster restart");
		TestUtils.startZookeeper();
		
		ClusterStatusProvider statusProvider = new StaticStatusProvider(Collections.singletonMap(TestUtils.metaResourceName, 1));
		ContainerProvider containerProvider = TestUtils.makeLocalProvider("test");
		ContainerStatusProvider containerStatusProvider = new LocalContainerStatusProvider();
		
		TestUtils.startTestCluster(statusProvider, containerStatusProvider, containerProvider);
		TestUtils.stopTestCluster();

		TestUtils.startTestCluster(statusProvider, containerStatusProvider, containerProvider);
		TestUtils.stopTestCluster();

		TestUtils.stopZookeeper();
	}

}
