package org.apache.helix.autoscale.impl.local;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.autoscale.ContainerProvider;
import org.apache.helix.autoscale.ContainerProviderService;
import org.apache.helix.autoscale.container.ContainerProcess;
import org.apache.helix.autoscale.container.ContainerProcessProperties;
import org.apache.helix.autoscale.container.ContainerUtils;
import org.apache.helix.autoscale.impl.local.LocalContainerSingleton.LocalProcess;
import org.apache.helix.autoscale.provider.ProviderProperties;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * {@link ContainerProvider} spawning VM-local containers. Only works in single-VM
 * deployments as container metadata is managed via singleton.
 * 
 * @see LocalContainerSingleton
 */
class LocalContainerProvider implements ContainerProviderService {

    static final Logger           log   = Logger.getLogger(LocalContainerProvider.class);

    final Map<String, Properties> types = new HashMap<String, Properties>();

    String                        address;
    String                        cluster;
    String                        name;

    @Override
    public void configure(Properties properties) throws Exception {
        ProviderProperties providerProperties = new ProviderProperties();
        providerProperties.putAll(properties);
        Preconditions.checkArgument(providerProperties.isValid());

        this.address = providerProperties.getProperty("address");
        this.cluster = providerProperties.getProperty("cluster");
        this.name = providerProperties.getProperty("name");

        for (String containerType : providerProperties.getContainers()) {
            registerType(containerType, providerProperties.getContainer(containerType));
        }
    }

    @Override
    public void start() throws Exception {
        // left blank
    }

    @Override
    public void stop() throws Exception {
        destroyAll();
    }

    @Override
    public void create(String id, String type) throws Exception {
        Map<String, LocalProcess> processes = LocalContainerSingleton.getProcesses();

        synchronized (processes) {
            Preconditions.checkState(!processes.containsKey(id), "Process '%s' already exists", id);
            Preconditions.checkState(types.containsKey(type), "Type '%s' is not registered", type);

            ContainerProcessProperties properties = new ContainerProcessProperties(types.get(type));

            properties.setProperty(ContainerProcessProperties.CLUSTER, cluster);
            properties.setProperty(ContainerProcessProperties.NAME, id);
            properties.setProperty(ContainerProcessProperties.ADDRESS, address);

            log.info(String.format("Running container '%s' (properties='%s')", id, properties));

            ContainerProcess process = ContainerUtils.createProcess(properties);
            process.start();

            processes.put(id, new LocalProcess(id, name, process));

        }
    }

    @Override
    public void destroy(String id) throws Exception {
        Map<String, LocalProcess> processes = LocalContainerSingleton.getProcesses();

        synchronized (processes) {
            if (!processes.containsKey(id))
                throw new IllegalArgumentException(String.format("Process '%s' does not exists", id));

            log.info(String.format("Destroying container '%s'", id));

            LocalProcess local = processes.remove(id);

            local.process.stop();
        }
    }

    @Override
    public void destroyAll() {
        Map<String, LocalProcess> processes = LocalContainerSingleton.getProcesses();

        synchronized (processes) {
            log.info("Destroying all processes");
            for (LocalProcess local : new HashSet<LocalProcess>(processes.values())) {
                if (local.owner.equals(name)) {
                    try { destroy(local.id); } catch (Exception ignore) {}
                }
            }
        }
    }

    void registerType(String name, Properties properties) {
        log.debug(String.format("Registering container type '%s' (properties='%s')", name, properties));
        types.put(name, properties);
    }

}
