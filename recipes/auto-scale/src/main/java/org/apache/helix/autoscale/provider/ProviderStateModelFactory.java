package org.apache.helix.autoscale.provider;

import org.apache.helix.autoscale.ClusterAdmin;
import org.apache.helix.autoscale.ContainerProvider;
import org.apache.helix.participant.statemachine.StateModelFactory;

/**
 * Factory for {@link ProviderStateModel}. Injects {@link ClusterAdmin} for
 * managed cluster and {@link ContainerProvider}.
 * 
 */
class ProviderStateModelFactory extends StateModelFactory<ProviderStateModel> {

    final ContainerProvider provider;
    final ClusterAdmin      admin;

    public ProviderStateModelFactory(ContainerProvider provider, ClusterAdmin admin) {
        super();
        this.provider = provider;
        this.admin = admin;
    }

    @Override
    public ProviderStateModel createNewStateModel(String partitionName) {
        return new ProviderStateModel(provider, admin);
    }
}
