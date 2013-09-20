package org.apache.helix.metamanager.impl.shell;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.helix.metamanager.ContainerProvider;
import org.apache.helix.metamanager.ContainerProviderService;
import org.apache.helix.metamanager.container.ContainerProcessProperties;
import org.apache.helix.metamanager.impl.shell.ShellContainerSingleton.ShellProcess;
import org.apache.helix.metamanager.provider.ProviderProperties;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

/**
 * {@link ContainerProvider} spawning shell-based containers. Only works in single-VM
 * deployments as container metadata is managed via singleton.
 * 
 * @see ShellContainerSingleton
 */
class ShellContainerProvider implements ContainerProviderService {

    static final Logger                    log               = Logger.getLogger(ShellContainerProvider.class);

    static final String                    RUN_COMMAND       = "/bin/sh";

    static final long                      POLL_INTERVAL     = 1000;
    static final long                      CONTAINER_TIMEOUT = 60000;

    // global view of processes required
    static final Map<String, ShellProcess> processes         = new HashMap<String, ShellProcess>();

    final Map<String, Properties>          types             = new HashMap<String, Properties>();

    String                                 address;
    String                                 cluster;
    String                                 name;

    @Override
    public void configure(Properties properties) throws Exception {
        Preconditions.checkNotNull(properties);
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
        Map<String, ShellProcess> processes = ShellContainerSingleton.getProcesses();

        synchronized (processes) {
            Preconditions.checkState(!processes.containsKey(id), "Process '%s' already exists", id);
            Preconditions.checkState(types.containsKey(type), "Type '%s' is not registered", type);

            ContainerProcessProperties properties = new ContainerProcessProperties(types.get(type));

            properties.setProperty(ContainerProcessProperties.CLUSTER, cluster);
            properties.setProperty(ContainerProcessProperties.NAME, id);
            properties.setProperty(ContainerProcessProperties.ADDRESS, address);

            File tmpDir = Files.createTempDir();
            File tmpProperties = new File(tmpDir.getCanonicalPath() + File.separator + ShellUtils.SHELL_CONTAINER_PROPERTIES);
            File tmpMarker = new File(tmpDir.getCanonicalPath());

            properties.store(new FileWriter(tmpProperties), id);

            log.info(String.format("Running container '%s' (properties='%s')", id, properties));

            log.debug(String.format("Invoking command '%s %s %s %s'", RUN_COMMAND, ShellUtils.SHELL_CONTAINER_PATH, tmpProperties.getCanonicalPath(),
                    tmpMarker.getCanonicalPath()));

            ProcessBuilder builder = new ProcessBuilder();
            builder.command(RUN_COMMAND, ShellUtils.SHELL_CONTAINER_PATH, tmpProperties.getCanonicalPath(), tmpMarker.getCanonicalPath());

            Process process = builder.start();

            processes.put(id, new ShellProcess(id, name, process, tmpDir));

            long limit = System.currentTimeMillis() + CONTAINER_TIMEOUT;
            while (!ShellUtils.hasMarker(tmpDir)) {
                if (System.currentTimeMillis() >= limit) {
                    throw new TimeoutException(String.format("Container '%s' failed to reach active state", id));
                }
                Thread.sleep(POLL_INTERVAL);
            }
        }
    }

    @Override
    public void destroy(String id) throws Exception {
        Map<String, ShellProcess> processes = ShellContainerSingleton.getProcesses();

        synchronized (processes) {
            if (!processes.containsKey(id))
                throw new IllegalArgumentException(String.format("Process '%s' does not exists", id));

            log.info(String.format("Destroying container '%s'", id));

            ShellProcess shell = processes.remove(id);
            shell.process.destroy();
            shell.process.waitFor();

            FileUtils.deleteDirectory(shell.tmpDir);
        }
    }

    @Override
    public void destroyAll() {
        Map<String, ShellProcess> processes = ShellContainerSingleton.getProcesses();

        synchronized (processes) {
            log.info("Destroying all owned processes");
            for (ShellProcess process : new HashSet<ShellProcess>(processes.values())) {
                if (process.owner.equals(name)) {
			        try { destroy(process.id); } catch (Exception ignore) {}
                }
            }
        }
    }

    void registerType(String name, Properties properties) {
        log.debug(String.format("Registering container type '%s' (properties='%s')", name, properties));
        types.put(name, properties);
    }

}
