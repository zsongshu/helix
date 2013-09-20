package org.apache.helix.metamanager.integration;

import java.util.Collections;
import java.util.Iterator;

import org.apache.helix.metamanager.Service;
import org.apache.helix.metamanager.TestUtils;
import org.apache.helix.metamanager.impl.StaticTargetProvider;
import org.apache.helix.metamanager.impl.local.LocalContainerProviderProcess;
import org.apache.helix.metamanager.impl.local.LocalContainerSingleton;
import org.apache.helix.metamanager.impl.local.LocalStatusProvider;
import org.apache.helix.metamanager.impl.shell.ShellContainerProviderProcess;
import org.apache.helix.metamanager.impl.shell.ShellContainerSingleton;
import org.apache.helix.metamanager.impl.shell.ShellStatusProvider;
import org.apache.helix.metamanager.impl.yarn.YarnContainerProviderProcess;
import org.apache.helix.metamanager.impl.yarn.YarnStatusProvider;
import org.apache.helix.metamanager.impl.yarn.ZookeeperMetadataProvider;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FailoverIT {
	
	static final Logger log = Logger.getLogger(FailoverIT.class);
	
	static final int CONTAINER_COUNT = 7;

	StaticTargetProvider targetProvider;
	YarnStatusProvider yarnStatusProvider;
	
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
		targetProvider = new StaticTargetProvider(Collections.singletonMap(TestUtils.metaResourceName, CONTAINER_COUNT));
	}
	
	@AfterMethod
	public void teardownTest() throws Exception {
		TestUtils.stopTestCluster();
		
		if(yarnStatusProvider != null) {
		    yarnStatusProvider.stop();
		    yarnStatusProvider = null;
		}
		
		TestUtils.stopZookeeper();
	}
	
    @Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testLocalContainerFailover() throws Exception {
		log.info("testing local container failover");
		TestUtils.startTestCluster(targetProvider, new LocalStatusProvider(), makeLocalProviders(3));
		killLocalContainers();
	}
	
    @Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testLocalProviderFailover() throws Exception {
		log.info("testing local provider failover");
		TestUtils.startTestCluster(targetProvider, new LocalStatusProvider(), makeLocalProviders(3));
		killProvider();
	}
	
    @Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testShellContainerFailover() throws Exception {
		log.info("testing shell container failover");
		TestUtils.startTestCluster(targetProvider, new ShellStatusProvider(), makeShellProviders(3));
		killShellContainers();
	}
	
    @Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testShellProviderFailover() throws Exception {
		log.info("testing shell provider failover");
		TestUtils.startTestCluster(targetProvider, new ShellStatusProvider(), makeShellProviders(3));
		killProvider();
	}
	
    @Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testYarnContainerFailover() throws Exception {
		log.info("testing yarn container failover");
		yarnStatusProvider = new YarnStatusProvider(TestUtils.zkAddress);
		yarnStatusProvider.start();
		TestUtils.startTestCluster(targetProvider, yarnStatusProvider, makeYarnProviders(3));
		killYarnContainers();
	}
	
    @Test(timeOut = TestUtils.TEST_TIMEOUT)
	public void testYarnProviderFailover() throws Exception {
		log.info("testing yarn provider failover");
		yarnStatusProvider = new YarnStatusProvider(TestUtils.zkAddress);
		yarnStatusProvider.start();
		TestUtils.startTestCluster(targetProvider, yarnStatusProvider, makeYarnProviders(3));
		killProvider();
	}
	
	void killLocalContainers() throws Exception {
	    LocalContainerSingleton.killProcess("container_2");
        LocalContainerSingleton.killProcess("container_4");
        LocalContainerSingleton.killProcess("container_6");
        Thread.sleep(3000);
		TestUtils.rebalanceTestCluster();
		TestUtils.waitUntilRebalancedCount(CONTAINER_COUNT);
	}
	
    void killShellContainers() throws Exception {
        ShellContainerSingleton.killProcess("container_2");
        ShellContainerSingleton.killProcess("container_4");
        ShellContainerSingleton.killProcess("container_6");
        Thread.sleep(3000);
        TestUtils.rebalanceTestCluster();
        TestUtils.waitUntilRebalancedCount(CONTAINER_COUNT);
    }
    
    void killYarnContainers() throws Exception {
        ZookeeperMetadataProvider metadata = new ZookeeperMetadataProvider(TestUtils.zkAddress);
        metadata.start();
        metadata.delete("container_2");
        metadata.delete("container_4");
        metadata.delete("container_6");
        metadata.stop();
        Thread.sleep(3000);
        TestUtils.rebalanceTestCluster();
        TestUtils.waitUntilRebalancedCount(CONTAINER_COUNT);
    }
    
	static void killProvider() throws Exception {
		Iterator<Service> itService = TestUtils.providerServices.iterator();
		itService.next().stop();
		itService.remove();
		
		TestUtils.rebalanceTestCluster();
		TestUtils.waitUntilRebalancedCount(CONTAINER_COUNT);
	}
	
	LocalContainerProviderProcess[] makeLocalProviders(int count) throws Exception {
	    LocalContainerProviderProcess[] localProviders = new LocalContainerProviderProcess[count];
		for(int i=0; i<count; i++) {
		    localProviders[i] = TestUtils.makeLocalProvider("provider_" + i);
		}
		return localProviders;
	}
	
	ShellContainerProviderProcess[] makeShellProviders(int count) throws Exception {
	    ShellContainerProviderProcess[] shellProviders = new ShellContainerProviderProcess[count];
		for(int i=0; i<count; i++) {
		    shellProviders[i] = TestUtils.makeShellProvider("provider_" + i);
		}
		return shellProviders;
	}
	
	YarnContainerProviderProcess[] makeYarnProviders(int count) throws Exception {
	    YarnContainerProviderProcess[] yarnProviders = new YarnContainerProviderProcess[count];
		for(int i=0; i<count; i++) {
		    yarnProviders[i] = TestUtils.makeYarnProvider("provider_" + i);
		}
		return yarnProviders;
	}
	
}
