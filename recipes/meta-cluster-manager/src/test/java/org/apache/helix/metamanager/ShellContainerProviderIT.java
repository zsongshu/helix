package org.apache.helix.metamanager;

import java.util.Collections;

import org.apache.helix.metamanager.impl.StaticTargetProvider;
import org.apache.helix.metamanager.impl.shell.ShellContainerProviderProcess;
import org.apache.helix.metamanager.impl.shell.ShellStatusProvider;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Shell container provider and shell status provider test. Scale-up and -down
 * only, no failures.
 * 
 * @see ShellContainerProvider
 * @see ShellStatusProvider
 */
@Test(groups = { "integration", "shell" })
public class ShellContainerProviderIT {

    static final Logger           log               = Logger.getLogger(ShellContainerProviderIT.class);

    static final long             TEST_TIMEOUT      = 20000;
    static final long             REBALANCE_TIMEOUT = 10000;

    static final int              CONTAINER_COUNT   = 4;

    StaticTargetProvider          clusterStatusProvider;
    ShellContainerProviderProcess containerProvider;
    ShellStatusProvider           containerStatusProvider;
	
	@BeforeClass(alwaysRun = true)
	public void setupClass() {
		log.info("installing shutdown hook");
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try { teardownTest(); } catch(Exception ignore) {};
			}
		}));
	}
	
    @BeforeMethod(alwaysRun = true)
    public void setupTest() throws Exception {
        teardownTest();
        TestUtils.configure();
        TestUtils.startZookeeper();
        containerProvider = TestUtils.makeShellProvider("provider_0");
        clusterStatusProvider = new StaticTargetProvider(Collections.singletonMap(TestUtils.metaResourceName, CONTAINER_COUNT));
        containerStatusProvider = new ShellStatusProvider();
        TestUtils.startTestCluster(clusterStatusProvider, containerStatusProvider, containerProvider);
    }

    @AfterMethod(alwaysRun = true)
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
