package org.apache.helix.autoscale;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.autoscale.container.ContainerProcess;
import org.apache.helix.autoscale.container.ContainerProcessProperties;
import org.apache.helix.autoscale.impl.container.RedisServerProcess.RedisServerModelFactory;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.Workflow;

import com.google.common.base.Preconditions;

public abstract class HelixYarnProcess extends ContainerProcess {
	protected final String _appName;
	private String _taskClassName;
	
	public HelixYarnProcess(ContainerProcessProperties properties) throws Exception {
        configure(properties);
        setModelName("Task");

        _appName = properties.getProperty(ContainerProcessProperties.NAME);
		_taskClassName = properties.getProperty("taskClass");

    }
	
    protected void startContainer() throws Exception {
    }

    protected void startParticipant() throws Exception {
        participantManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zookeeperAddress);
        
    }
    

	private Map<String, TaskFactory> getTaskFactoryReg(String appName, String taskClassName, Properties taskConfig) {
	    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
	    taskFactoryReg.put(_appName, new TaskFactory()
	    {
	      @Override
	      public Task createNewTask(String config)
	      {
	        return null;
	      }
	    });
	    
	    return taskFactoryReg;
	}
}
