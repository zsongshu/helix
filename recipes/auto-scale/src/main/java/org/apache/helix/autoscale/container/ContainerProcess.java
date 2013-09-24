package org.apache.helix.autoscale.container;

import java.util.Properties;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.autoscale.Service;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Base service for spawn-able container types. Configure from Properties and
 * instantiates Helix participant to managed cluster.
 * 
 */
public abstract class ContainerProcess implements Service {
    static final Logger                             log    = Logger.getLogger(ContainerProcess.class);

    private ContainerProcessProperties              properties;
    protected HelixManager                            participantManager;

    protected String                                  modelName;
    protected StateModelFactory<? extends StateModel> modelFactory;

    protected String                                  instanceName;
    protected String                                  clusterName;
    protected String                                  zookeeperAddress;

    private boolean                                 active = false;
    private boolean                                 failed = false;

    public final void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public final void setModelFactory(StateModelFactory<? extends StateModel> modelFactory) {
        this.modelFactory = modelFactory;
    }

    @Override
    public void configure(Properties properties) throws Exception {
        ContainerProcessProperties containerProps = new ContainerProcessProperties();
        containerProps.putAll(properties);
        Preconditions.checkArgument(containerProps.isValid());

        this.properties = containerProps;
        this.instanceName = containerProps.getName();
        this.clusterName = containerProps.getCluster();
        this.zookeeperAddress = containerProps.getAddress();
    }

    @Override
    public final void start() {
        try {
            Preconditions.checkNotNull(modelName, "state model name not set");
            Preconditions.checkNotNull(modelFactory, "state model factory not set");
            Preconditions.checkState(properties.isValid(), "process properties not valid: %s", properties.toString());

            log.info(String.format("starting container '%s'", instanceName));
            startContainer();

            log.info(String.format("starting helix participant '%s'", instanceName));
            startParticipant();

            active = true;

        } catch (Exception e) {
            log.error(String.format("starting container '%s' failed", instanceName), e);
            fail();
        }
    }

    protected abstract void startContainer() throws Exception;

    protected void startParticipant() throws Exception {
        participantManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zookeeperAddress);
        participantManager.getStateMachineEngine().registerStateModelFactory(modelName, modelFactory);
        participantManager.connect();
    }

    @Override
    public final void stop() {
        try {
            log.info(String.format("stopping helix participant '%s'", instanceName));
            stopParticipant();

            log.info(String.format("stopping container '%s'", instanceName));
            stopContainer();

            active = false;

        } catch (Exception e) {
            log.warn(String.format("stopping container '%s' failed", instanceName), e);
        }
    }

    protected abstract void stopContainer() throws Exception;

    private final void stopParticipant() {
        if (participantManager != null) {
            participantManager.disconnect();
        }
    }

    public final void fail() {
        failed = true;
    }

    public final boolean isActive() {
        return active && !failed;
    }

    public final boolean isFailed() {
        return failed;
    }

    public final ContainerProcessProperties getProperties() {
        return properties;
    }

    String getModelName() {
        return modelName;
    }

    StateModelFactory<? extends StateModel> getModelFactory() {
        return modelFactory;
    }

}
