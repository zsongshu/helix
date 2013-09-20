package org.apache.helix.autoscale;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.helix.HelixAdmin;
import org.apache.helix.autoscale.Service;
import org.apache.helix.autoscale.bootstrapper.Boot;
import org.apache.helix.autoscale.bootstrapper.ClusterService;
import org.apache.helix.autoscale.bootstrapper.ControllerService;
import org.apache.helix.autoscale.bootstrapper.MetaClusterService;
import org.apache.helix.autoscale.bootstrapper.MetaControllerService;
import org.apache.helix.autoscale.bootstrapper.MetaProviderService;
import org.apache.helix.autoscale.bootstrapper.MetaResourceService;
import org.apache.helix.autoscale.bootstrapper.ResourceService;
import org.apache.helix.autoscale.bootstrapper.ZookeeperService;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Bootstrapping tool test. Reads cluster configuration from *.properties files,
 * spawns services and verifies number of active partitions and containers
 * 
 * @see Boot
 */
@Test(groups = { "integration", "boot" })
public class BootstrapperIT {

    static final Logger log = Logger.getLogger(BootstrapperIT.class);

    Boot                boot;
    HelixAdmin          admin;

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        log.debug("tearing down bootstrap test");
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (boot != null) {
            boot.stop();
            boot = null;
        }
    }

    @Test(timeOut = TestUtils.TEST_TIMEOUT, groups = { "local" })
    public void bootstrapLocalTest() throws Exception {
        boot = new Boot();
        boot.configure(getProperties("BootLocal.properties"));
        boot.start();

        Assert.assertTrue(containsInstanceOf(boot.getServcies(), ZookeeperService.class));
        Assert.assertTrue(containsInstanceOf(boot.getServcies(), ClusterService.class));
        Assert.assertTrue(containsInstanceOf(boot.getServcies(), ResourceService.class));
        Assert.assertTrue(containsInstanceOf(boot.getServcies(), ControllerService.class));
        Assert.assertTrue(containsInstanceOf(boot.getServcies(), MetaClusterService.class));
        Assert.assertTrue(containsInstanceOf(boot.getServcies(), MetaResourceService.class));
        Assert.assertTrue(containsInstanceOf(boot.getServcies(), MetaProviderService.class));
        Assert.assertTrue(containsInstanceOf(boot.getServcies(), MetaControllerService.class));

        final long limit = System.currentTimeMillis() + TestUtils.REBALANCE_TIMEOUT;

        admin = new ZKHelixAdmin("localhost:2199");
        waitUntil(admin, "meta", "container", 1, 7, (limit - System.currentTimeMillis()));
        waitUntil(admin, "cluster", "resource", 7, 10, (limit - System.currentTimeMillis()));

    }

    @Test(timeOut = TestUtils.TEST_TIMEOUT, groups = { "local" })
    public void bootstrap2By2LocalTest() throws Exception {
        boot = new Boot();
        boot.configure(getProperties("Boot2By2Local.properties"));
        boot.start();

        verify2By2Setup();
    }

    @Test(timeOut = TestUtils.TEST_TIMEOUT, groups = { "shell" })
    public void bootstrap2By2ShellTest() throws Exception {
        boot = new Boot();
        boot.configure(getProperties("Boot2By2Shell.properties"));
        boot.start();

        verify2By2Setup();
    }

    @Test(timeOut = TestUtils.TEST_TIMEOUT, groups = { "yarn" })
    public void bootstrap2By2YarnTest() throws Exception {
        boot = new Boot();
        boot.configure(getProperties("Boot2By2Yarn.properties"));
        boot.start();

        verify2By2Setup();
    }

    void verify2By2Setup() throws Exception {
        final long limit = System.currentTimeMillis() + TestUtils.REBALANCE_TIMEOUT;
        final String address = "localhost:2199";

        log.debug(String.format("connecting to zookeeper at '%s'", address));

        admin = new ZKHelixAdmin(address);
        waitUntil(admin, "meta", "database", 2, 3, (limit - System.currentTimeMillis()));
        waitUntil(admin, "meta", "webserver", 2, 5, (limit - System.currentTimeMillis()));
        waitUntil(admin, "cluster", "dbprod", 3, 8, (limit - System.currentTimeMillis()));
        waitUntil(admin, "cluster", "wsprod", 5, 15, (limit - System.currentTimeMillis()));
    }

    static void waitUntil(HelixAdmin admin, String cluster, String resource, int instanceCount, int partitionCount, long timeout) throws Exception {
        final long limit = System.currentTimeMillis() + timeout;
        TestUtils.waitUntilInstanceCount(admin, cluster, resource, instanceCount, (limit - System.currentTimeMillis()));
        TestUtils.waitUntilPartitionCount(admin, cluster, resource, partitionCount, (limit - System.currentTimeMillis()));
    }

    static Properties getProperties(String resourcePath) throws IOException {
        Properties properties = new Properties();
        properties.load(ClassLoader.getSystemResourceAsStream(resourcePath));
        return properties;
    }

    static boolean containsInstanceOf(Collection<Service> services, Class<?> clazz) {
        for (Service service : services) {
            if (clazz.isAssignableFrom(service.getClass()))
                return true;
        }
        return false;
    }

}
