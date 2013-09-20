package org.apache.helix.metamanager.container.impl;

import org.apache.helix.NotificationContext;
import org.apache.helix.metamanager.container.ContainerProcess;
import org.apache.helix.metamanager.container.ContainerProcessProperties;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

public class DummyMasterSlaveProcess extends ContainerProcess {

	static final Logger log = Logger.getLogger(DummyMasterSlaveProcess.class);
	
	public DummyMasterSlaveProcess(ContainerProcessProperties properties) {
		super(properties);
		setModelName("MasterSlave");
		setModelFactory(new DummyMasterSlaveModelFactory());
	}

	@Override
	protected void startContainer() throws Exception {
		log.info("starting dummy process container");
	}

	@Override
	protected void stopContainer() throws Exception {
		log.info("stopping dummy process container");
	}

	public static class DummyMasterSlaveModelFactory extends StateModelFactory<DummyMasterSlaveStateModel> {
		@Override
		public DummyMasterSlaveStateModel createNewStateModel(String partitionName) {
			return new DummyMasterSlaveStateModel();
		}
	}
	
	@StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "SLAVE", "MASTER", "DROPPED" })
	public static class DummyMasterSlaveStateModel extends StateModel {
		
		static final Logger log = Logger.getLogger(DummyMasterSlaveStateModel.class);
		
		@Transition(from = "OFFLINE", to = "SLAVE")
		public void offlineToSlave(Message m, NotificationContext context) {
			log.trace(String.format("%s transitioning from OFFLINE to SLAVE",
					context.getManager().getInstanceName()));
		}

		@Transition(from = "SLAVE", to = "OFFLINE")
		public void slaveToOffline(Message m, NotificationContext context) {
			log.trace(String.format("%s transitioning from SLAVE to OFFLINE",
					context.getManager().getInstanceName()));
		}

		@Transition(from = "SLAVE", to = "MASTER")
		public void slaveToMaster(Message m, NotificationContext context) {
			log.trace(String.format("%s transitioning from SLAVE to MASTER",
					context.getManager().getInstanceName()));
		}

		@Transition(from = "MASTER", to = "SLAVE")
		public void masterToSlave(Message m, NotificationContext context) {
			log.trace(String.format("%s transitioning from MASTER to SLAVE",
					context.getManager().getInstanceName()));
		}

		@Transition(from = "OFFLINE", to = "DROPPED")
		public void offlineToDropped(Message m, NotificationContext context) {
			log.trace(String.format("%s transitioning from OFFLINE to DROPPED",
					context.getManager().getInstanceName()));
		}

	}
}
