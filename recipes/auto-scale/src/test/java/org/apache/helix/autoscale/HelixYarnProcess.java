package org.apache.helix.autoscale;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.autoscale.container.ContainerProcess;
import org.apache.helix.autoscale.container.ContainerProcessProperties;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.log4j.Logger;


public abstract class HelixYarnProcess extends ContainerProcess {
	protected final String _appName;
	private String _taskClassName;
	static final Logger log = Logger.getLogger(HelixYarnProcess.class);

	
	public HelixYarnProcess(ContainerProcessProperties properties) throws Exception {
        configure(properties);
        setModelName("Task");

        _appName = properties.getProperty(ContainerProcessProperties.NAME);
		_taskClassName = properties.getProperty("taskClass");

    }
	
    protected void startContainer() throws Exception {
    }

    protected final void startParticipant() throws Exception {
        participantManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zookeeperAddress);
        modelName = "Task";
        modelFactory = new TaskStateModelFactory(participantManager, getTaskFactoryReg());
        participantManager.getStateMachineEngine().registerStateModelFactory(modelName, modelFactory);
    }
    

	private Map<String, TaskFactory> getTaskFactoryReg() {
	    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
	    taskFactoryReg.put(_appName, new TaskFactory()
	    {
	      @Override
	      public Task createNewTask(String config)
	      {
	    	Class<?> clazz;
			try {
				clazz = Class.forName(_taskClassName);
		        Task task = createInstance(clazz);
		        //Properties taskConfig = new Properties(); //TODO: Get this from participantManager
		        //task.configure(taskConfig);
		        return task;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
	      }
	    });
	    
	    return taskFactoryReg;
	}
	
    @SuppressWarnings("unchecked")
    private <T> T createInstance(Class<?> clazz) throws Exception {
        try {
            log.debug(String.format("checking for default constructor in class '%s'", clazz.getSimpleName()));
            return (T) clazz.getConstructor().newInstance();
        } catch (Exception e) {
            log.debug("no default constructor found");
        }

        throw new Exception(String.format("no suitable constructor for class '%s'", clazz.getSimpleName()));
    }
}
