package org.apache.helix.autoscale;

import java.io.IOException;
import java.util.Properties;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.autoscale.impl.yarn.YarnContainerProviderProcess;
import org.apache.helix.autoscale.impl.yarn.YarnStatusProvider;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.Workflow;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HelixYarnTest {
    
	static final Logger           log               = Logger.getLogger(HelixYarnTest.class);

    static final long             TEST_TIMEOUT      = 20000;
    static final long             REBALANCE_TIMEOUT = 10000;


    YarnContainerProviderProcess _containerProvider;
    YarnStatusProvider           _containerStatusProvider;
	
    @BeforeMethod(alwaysRun = true)
    public void setupTest() throws Exception {
        teardownTest();
        TestUtils.configure();
        TestUtils.startZookeeper();
        _containerProvider = TestUtils.makeYarnProvider("provider_0");
		_containerStatusProvider = new YarnStatusProvider(TestUtils.zkAddress);
    }

    @AfterMethod(alwaysRun = true)
    public void teardownTest() throws Exception {
        TestUtils.stopTestCluster();
        TestUtils.stopZookeeper();
    }
    
    @Test(timeOut = TestUtils.TEST_TIMEOUT)
    public void testBasic() throws Exception {
    	Properties applicationSpec = getProperties("myapp.properties");
    	TargetProviderService targetProvider = getTargetProviderService(applicationSpec);
        TestUtils.startTestCluster(targetProvider, _containerStatusProvider, _containerProvider);

        TestUtils.admin.addResource(TestUtils.managedClusterName, WorkflowGenerator.DEFAULT_TGT_DB, 2, "MasterSlave");
        
    	HelixManager taskClusterManager = HelixManagerFactory.getZKHelixManager(TestUtils.managedClusterName, 
    			"Admin", InstanceType.ADMINISTRATOR, TestUtils.zkAddress);
    	taskClusterManager.connect();
    	TaskDriver driver = new TaskDriver(taskClusterManager);
		/*Workflow flow = WorkflowGenerator.generateDefaultSingleTaskWorkflowBuilderWithExtraConfigs(taskName, TaskConfig.COMMAND_CONFIG,
						String.valueOf(100)).setExpiry(expiry).build();

		driver.start(flow);*/
    	//TODO: Submit Workflow using TaskDriver 
    }
    
    private TargetProviderService getTargetProviderService(Properties applicationSpec) throws Exception {
    	String className = applicationSpec.getProperty("targetProviderClass");
    	Class<?> clazz = Class.forName(className);
    	return createInstance(clazz);
	}

    @SuppressWarnings("unchecked")
    public static <T> T createInstance(Class<?> clazz) throws Exception {
        try {
            log.debug(String.format("checking for default constructor in class '%s'", clazz.getSimpleName()));
            return (T) clazz.getConstructor().newInstance();
        } catch (Exception e) {
            log.debug("no default constructor found");
        }

        throw new Exception(String.format("no suitable constructor for class '%s'", clazz.getSimpleName()));
    }
    
	static Properties getProperties(String resourcePath) throws IOException {
        Properties properties = new Properties();
        properties.load(ClassLoader.getSystemResourceAsStream(resourcePath));
        return properties;
    }
}
