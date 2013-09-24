package org.apache.helix.autoscale;

import java.util.Properties;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;

public class MyAppTask implements Task {
	@Override
	public TaskResult run() {
		System.out.println("Task running");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new TaskResult(TaskResult.Status.COMPLETED, "Done");
	}

	@Override
	public void cancel() {
	}

	//@Override
	public void configure(Properties config) {
	}
}
