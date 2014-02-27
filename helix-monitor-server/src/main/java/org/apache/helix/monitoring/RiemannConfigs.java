package org.apache.helix.monitoring;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.helix.model.MonitoringConfig;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * Riemann configs
 */
public class RiemannConfigs {
  private static final Logger LOG = Logger.getLogger(RiemannConfigs.class);
  private static final String DEFAULT_CONFIG_DIR = "riemannconfigs";
  public static final String DEFAULT_RIEMANN_CONFIG = "riemann.config";

  private final String _configDir;
  private final List<MonitoringConfig> _configs;

  RiemannConfigs(String configDir, List<MonitoringConfig> configs) {
    _configDir = configDir;
    _configs = configs;
  }

  /**
   * persist configs to riemann config dir
   */
  public void persistConfigs() {
    // create the directory
    File dir = new File(_configDir);
    if (!dir.exists()) {
      dir.mkdir();
    }

    for (MonitoringConfig config : _configs) {
      String configData = config.getConfig();
      String fileName = _configDir + "/" + config.getId();
      try {
        PrintWriter writer = new PrintWriter(fileName);
        writer.println(configData);
        writer.close();

        // make sure this is cleaned up eventually
        File file = new File(fileName);
        file.deleteOnExit();
      } catch (FileNotFoundException e) {
        LOG.error("Could not write " + config.getId(), e);
      }
    }
  }

  public String getConfigDir() {
    return _configDir;
  }

  public static class Builder {
    private final List<MonitoringConfig> _configs;
    private final String _configDir;

    /**
     * By default, configs will be placed in "{systemTmpDir}/riemannconfigs"
     */
    public Builder() {
      this(System.getProperty("java.io.tmpdir") + "/" + DEFAULT_CONFIG_DIR);
    }

    public Builder(String configDir) {
      _configDir = configDir;
      _configs = Lists.newArrayList();
    }

    public Builder addConfig(MonitoringConfig monitoringConfig) {
      _configs.add(monitoringConfig);
      return this;
    }

    public Builder addConfigs(List<MonitoringConfig> monitoringConfigs) {
      _configs.addAll(monitoringConfigs);
      return this;
    }

    public RiemannConfigs build() {
      // Check default riemann config exists
      for (MonitoringConfig config : _configs) {
        if (config.getId().equals(DEFAULT_RIEMANN_CONFIG)) {
          return new RiemannConfigs(_configDir, _configs);
        }
      }
      throw new IllegalArgumentException("Missing default riemann config: "
          + DEFAULT_RIEMANN_CONFIG);
    }
  }
}
