package org.apache.helix.autoscale.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.helix.autoscale.TargetProviderService;
import org.apache.log4j.Logger;

/**
 * Target model based on manually set count. Count may change dynamically.
 * 
 */
public class StaticTargetProvider implements TargetProviderService {
    static final Logger        log          = Logger.getLogger(StaticTargetProvider.class);

    final Map<String, Integer> targetCounts = new HashMap<String, Integer>();

    public StaticTargetProvider() {
        // left blank
    }

    public StaticTargetProvider(Map<String, Integer> targetCounts) {
        this.targetCounts.putAll(targetCounts);
    }

    @Override
    public int getTargetContainerCount(String containerType) {
        return targetCounts.get(containerType);
    }

    public void setTargetContainerCount(String containerType, int targetCount) {
        targetCounts.put(containerType, targetCount);
    }

    @Override
    public void configure(Properties properties) throws Exception {
        for (Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();

            try {
                int value = Integer.valueOf((String) entry.getValue());
                log.debug(String.format("Inserting value '%s = %d'", key, value));
                targetCounts.put(key, value);
            } catch (NumberFormatException e) {
                log.warn(String.format("Skipping '%s', not an integer (value='%s')", key, (String) entry.getValue()));
            }
        }
    }

    @Override
    public void start() throws Exception {
        // left blank
    }

    @Override
    public void stop() throws Exception {
        // left blank
    }

}
