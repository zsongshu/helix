package org.apache.helix.autoscale.impl.yarn;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

	@Override
	public void onContainerStarted(ContainerId containerId,
			Map<String, ByteBuffer> allServiceResponse) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onContainerStatusReceived(ContainerId containerId,
			ContainerStatus containerStatus) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onContainerStopped(ContainerId containerId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable t) {
		// TODO Auto-generated method stub
		
	}

}
