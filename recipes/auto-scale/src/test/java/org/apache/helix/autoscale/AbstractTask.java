package org.apache.helix.autoscale;
import java.util.Properties;

import org.apache.helix.task.Task;

public abstract class AbstractTask implements Task {
	protected Properties _taskConfig;
	
	public AbstractTask(Properties config) {
		_taskConfig = config;
	}
}
