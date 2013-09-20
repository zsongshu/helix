package org.apache.helix.metamanager;

import java.util.Collections;

import org.apache.helix.metamanager.Service;
import org.apache.helix.metamanager.StatusProviderService;
import org.apache.helix.metamanager.TargetProviderService;
import org.apache.helix.metamanager.impl.StaticTargetProvider;
import org.apache.helix.metamanager.impl.local.LocalStatusProvider;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

/**
 * Self-test of test cluster. Spawning zookeeper and cluster with single provider and single instance.
 * 
 * @see TestUtils
 */
@Test(groups={"unit"})
public class TestUtilsUT {

	static final Logger log = Logger.getLogger(TestUtilsUT.class);
	
	@Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testZookeeper() throws Exception {
		log.info("testing zookeeper");
	    TestUtils.configure();
		TestUtils.startZookeeper();
		TestUtils.stopZookeeper();
	}

	@Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testCluster() throws Exception {
		log.info("testing cluster");
        TestUtils.configure();
		TestUtils.startZookeeper();
		
		TestUtils.startTestCluster(new StaticTargetProvider(Collections.singletonMap(TestUtils.metaResourceName, 1)),
		        new LocalStatusProvider(), TestUtils.makeLocalProvider("test"));
		TestUtils.stopTestCluster();
		
		TestUtils.stopZookeeper();
	}

	@Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testClusterRepeated() throws Exception {
		log.info("testing cluster restart");
        TestUtils.configure();
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
