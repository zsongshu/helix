package org.apache.helix.metamanager;

import org.apache.helix.metamanager.provider.local.LocalContainerProvider;
import org.apache.helix.metamanager.provider.local.LocalContainerSingleton;

public class TestContainerProvider extends LocalContainerProvider {

	public TestContainerProvider(String providerName) {
		super(TestUtils.zkAddress, TestUtils.managedClusterName, providerName);
	}

	public void destroyAll() {
		super.destroyAll();
		LocalContainerSingleton.reset();
	}

}
