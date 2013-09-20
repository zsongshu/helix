package org.apache.helix.metamanager.impl.container;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.NotificationContext;
import org.apache.helix.metamanager.container.ContainerProcess;
import org.apache.helix.metamanager.container.ContainerProcessProperties;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

/**
 * Sample implementation of container with Helix participant for MasterSlave
 * state model. Writes current state to separate zookeeper domain.
 * 
 */
public class ZookeeperMasterSlaveProcess extends ContainerProcess {

    static final Logger log = Logger.getLogger(ZookeeperMasterSlaveProcess.class);

    ZkClient            zookeeper;

    final String        address;
    final String        root;
    final String        name;

    public ZookeeperMasterSlaveProcess(ContainerProcessProperties properties) throws Exception {
        configure(properties);
        setModelName("MasterSlave");
        setModelFactory(new ZookeeperMasterSlaveModelFactory());

        address = properties.getProperty("address");
        root = properties.getProperty("root");
        name = properties.getProperty(ContainerProcessProperties.NAME);
    }

    @Override
    protected void startContainer() throws Exception {
        log.info("starting zookeeper process container");

        zookeeper = new ZkClient(address);
        zookeeper.createPersistent("/" + root + "/" + name, true);
    }

    @Override
    protected void stopContainer() throws Exception {
        log.info("stopping zookeeper process container");

        zookeeper.close();
    }

    public class ZookeeperMasterSlaveModelFactory extends StateModelFactory<ZookeeperMasterSlaveModel> {
        @Override
        public ZookeeperMasterSlaveModel createNewStateModel(String partitionName) {
            return new ZookeeperMasterSlaveModel();
        }
    }

    @StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "SLAVE", "MASTER", "DROPPED" })
    public class ZookeeperMasterSlaveModel extends StateModel {

        @Transition(from = "OFFLINE", to = "SLAVE")
        public void offlineToSlave(Message m, NotificationContext context) {
            transition(m, context);
        }

        @Transition(from = "SLAVE", to = "OFFLINE")
        public void slaveToOffline(Message m, NotificationContext context) {
            transition(m, context);
        }

        @Transition(from = "SLAVE", to = "MASTER")
        public void slaveToMaster(Message m, NotificationContext context) {
            transition(m, context);
        }

        @Transition(from = "MASTER", to = "SLAVE")
        public void masterToSlave(Message m, NotificationContext context) {
            transition(m, context);
        }

        @Transition(from = "OFFLINE", to = "DROPPED")
        public void offlineToDropped(Message m, NotificationContext context) {
            log.trace(String.format("%s transitioning from %s to %s", context.getManager().getInstanceName(), m.getFromState(), m.getToState()));

            String resource = m.getResourceName();
            String partition = m.getPartitionName();
            String path = "/" + root + "/" + name + "/" + resource + "_" + partition;

            zookeeper.delete(path);
        }

        public void transition(Message m, NotificationContext context) {
            log.trace(String.format("%s transitioning from %s to %s", context.getManager().getInstanceName(), m.getFromState(), m.getToState()));

            String resource = m.getResourceName();
            String partition = m.getPartitionName();
            String path = "/" + root + "/" + name + "/" + resource + "_" + partition;

            zookeeper.delete(path);
            zookeeper.createEphemeral(path, m.getToState());
        }

    }

}
