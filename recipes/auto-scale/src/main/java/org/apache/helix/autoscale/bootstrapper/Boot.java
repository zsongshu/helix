package org.apache.helix.autoscale.bootstrapper;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.autoscale.Service;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Bootstrapper for elastic cluster deployment using *.properties configuration
 * files. (Program entry point)
 * 
 */
public class Boot implements Service {

    static final Logger       log          = Logger.getLogger(Boot.class);

    static final Map<String, Class<? extends Service>> classes      = new HashMap<String, Class<? extends Service>>();
    static {
        classes.put("zookeeper", ZookeeperService.class);
        classes.put("cluster", ClusterService.class);
        classes.put("resource", ResourceService.class);
        classes.put("controller", ControllerService.class);
        classes.put("metacluster", MetaClusterService.class);
        classes.put("metaresource", MetaResourceService.class);
        classes.put("metaprovider", MetaProviderService.class);
        classes.put("metacontroller", MetaControllerService.class);
    }

    static final List<String> serviceOrder = Arrays.asList("zookeeper", "cluster", "resource", "metacluster", "metaresource",
                                                                            "metaprovider", "controller", "metacontroller");

    Properties                properties;
    List<Service>             services     = Lists.newArrayList();

    @Override
    public void configure(Properties properties) throws Exception {
        Preconditions.checkNotNull(properties);
        this.properties = properties;
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("bootstraping started"));

        for (String key : serviceOrder) {
            if (BootUtils.hasNamespace(properties, key + ".0")) {
                processIndexedNamespace(key);
            } else if (BootUtils.hasNamespace(properties, key)) {
                processNamespace(key);
            }
        }

        log.info(String.format("bootstraping completed"));
    }

    private void processIndexedNamespace(String key) throws Exception {
        int i = 0;
        String indexedKey = key + "." + i;

        while (BootUtils.hasNamespace(properties, indexedKey)) {
            log.info(String.format("processing namespace '%s'", indexedKey));
            Service service = BootUtils.createInstance(classes.get(key));
            service.configure(BootUtils.getNamespace(properties, indexedKey));
            service.start();

            services.add(service);

            i++;
            indexedKey = key + "." + i;
        }
    }

    private void processNamespace(String key) throws Exception {
        log.info(String.format("processing namespace '%s'", key));
        Service service = BootUtils.createInstance(classes.get(key));
        service.configure(BootUtils.getNamespace(properties, key));
        service.start();

        services.add(service);
    }

    @Override
    public void stop() throws Exception {
        log.info(String.format("shutdown started"));

        Collections.reverse(services);
        for (Service service : services) {
            service.stop();
        }

        log.info(String.format("shutdown completed"));
    }

    public Collection<Service> getServcies() {
        return services;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            log.error(String.format("Usage: Boot properties_path"));
            return;
        }

        String resourcePath = args[0];

        log.info(String.format("reading definition from '%s'", resourcePath));
        Properties properties = new Properties();
        properties.load(ClassLoader.getSystemResourceAsStream(resourcePath));

        final Boot boot = new Boot();
        boot.configure(properties);
        boot.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.debug("Running shutdown hook");
                try { boot.stop(); } catch (Exception ignore) {}
            }
        }));
    }

}
