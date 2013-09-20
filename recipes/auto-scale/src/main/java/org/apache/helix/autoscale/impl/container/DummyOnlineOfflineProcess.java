package org.apache.helix.autoscale.impl.container;

import org.apache.helix.NotificationContext;
import org.apache.helix.autoscale.container.ContainerProcess;
import org.apache.helix.autoscale.container.ContainerProcessProperties;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

/**
 * Sample implementation of container with Helix participant for OnlineOffline
 * state model. Print state transitions only.
 * 
 */
public class DummyOnlineOfflineProcess extends ContainerProcess {

    static final Logger log = Logger.getLogger(DummyOnlineOfflineProcess.class);

    public DummyOnlineOfflineProcess(ContainerProcessProperties properties) throws Exception {
        configure(properties);
        setModelName("OnlineOffline");
        setModelFactory(new DummyOnlineOfflineModelFactory());
    }

    @Override
    protected void startContainer() throws Exception {
        log.info("starting dummy online-offline process container");
    }

    @Override
    protected void stopContainer() throws Exception {
        log.info("stopping dummy online-offline process container");
    }

    public static class DummyOnlineOfflineModelFactory extends StateModelFactory<DummyOnlineOfflineStateModel> {
        @Override
        public DummyOnlineOfflineStateModel createNewStateModel(String partitionName) {
            return new DummyOnlineOfflineStateModel();
        }
    }

    @StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "ONLINE", "DROPPED" })
    public static class DummyOnlineOfflineStateModel extends StateModel {

        static final Logger log = Logger.getLogger(DummyOnlineOfflineStateModel.class);

        @Transition(from = "OFFLINE", to = "ONLINE")
        public void offlineToOnline(Message m, NotificationContext context) {
            log.trace(String.format("%s transitioning from OFFLINE to ONLINE", context.getManager().getInstanceName()));
        }

        @Transition(from = "ONLINE", to = "OFFLINE")
        public void onlineToOffline(Message m, NotificationContext context) {
            log.trace(String.format("%s transitioning from ONLINE to OFFLINE", context.getManager().getInstanceName()));
        }

        @Transition(from = "OFFLINE", to = "DROPPED")
        public void offlineToDropped(Message m, NotificationContext context) {
            log.trace(String.format("%s transitioning from OFFLINE to DROPPED", context.getManager().getInstanceName()));
        }

    }
}
