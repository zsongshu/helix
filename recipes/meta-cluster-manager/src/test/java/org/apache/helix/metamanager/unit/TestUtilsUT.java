package org.apache.helix.metamanager.unit;

import java.util.Collections;

import org.apache.helix.metamanager.Service;
import org.apache.helix.metamanager.StatusProviderService;
import org.apache.helix.metamanager.TargetProviderService;
import org.apache.helix.metamanager.TestUtils;
import org.apache.helix.metamanager.impl.StaticTargetProvider;
import org.apache.helix.metamanager.impl.local.LocalStatusProvider;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

public class TestUtilsUT {

	static final Logger log = Logger.getLogger(TestUtilsUT.class);
	
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
		
		TestUtils.startTestCluster(new StaticTargetProvider(Collections.singletonMap(TestUtils.metaResourceName, 1)),
		        new LocalStatusProvider(), TestUtils.makeLocalProvider("test"));
		TestUtils.stopTestCluster();
		
		TestUtils.stopZookeeper();
	}

	@Test
	public void testClusterRepeated() throws Exception {
		log.info("testing cluster restart");
		TestUtils.startZookeeper();
		
		TargetProviderService statusProvider = new StaticTargetProvider(Collections.singletonMap(TestUtils.metaResourceName, 1));
		Service containerProvider = TestUtils.makeLocalProvider("test");
		StatusProviderService containerStatusProvider = new LocalStatusProvider();
		
		TestUtils.startTestCluster(statusProvider, containerStatusProvider, containerProvider);
		TestUtils.stopTestCluster();

		TestUtils.startTestCluster(statusProvider, containerStatusProvider, containerProvider);
		TestUtils.stopTestCluster();

		TestUtils.stopZookeeper();
	}

}
