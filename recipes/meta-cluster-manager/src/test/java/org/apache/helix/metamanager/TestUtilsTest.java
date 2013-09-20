package org.apache.helix.metamanager;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

public class TestUtilsTest {

	@Test
	public void testStartStop() throws Exception {
		TestUtils.startTestCluster(new TestStatusProvider(1),
				Collections.<ClusterContainerProvider>singletonList(new TestContainerProvider("test")));
		TestUtils.stopTestCluster();
	}

	@Test
	public void testStartStopRepeated() throws Exception {
		ClusterStatusProvider statusProvider = new TestStatusProvider(1);
		List<ClusterContainerProvider> containerProviders = Collections.<ClusterContainerProvider>singletonList(new TestContainerProvider("test"));
		
		TestUtils.startTestCluster(statusProvider, containerProviders);
		TestUtils.stopTestCluster();

		TestUtils.startTestCluster(statusProvider, containerProviders);
		TestUtils.stopTestCluster();

	}

}
