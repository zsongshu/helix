package org.apache.helix.autoscale.impl.local;

import java.util.Properties;

import org.apache.helix.autoscale.Service;
import org.apache.helix.autoscale.provider.ProviderProcess;
import org.apache.helix.autoscale.provider.ProviderProperties;

import com.google.common.base.Preconditions;

/**
 * Configurable and runnable service for {@link LocalContainerProvider}. 
 *
 */
public class LocalContainerProviderProcess implements Service {
    LocalContainerProvider provider;
    ProviderProcess        process;

    @Override
    public void configure(Properties properties) throws Exception {
        ProviderProperties providerProperties = new ProviderProperties();
        providerProperties.putAll(properties);
        
        Preconditions.checkArgument(providerProperties.isValid(), "provider properties not valid (properties='%s')", properties);
        
        provider = new LocalContainerProvider();
        provider.configure(properties);

        process = new ProviderProcess();
        process.configure(providerProperties);
        process.setConteinerProvider(provider);
    }

    @Override
    public void start() throws Exception {
        provider.start();
        process.start();
    }

    @Override
    public void stop() throws Exception {
        process.stop();
        provider.stop();
    }
}
