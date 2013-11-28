package org.apache.helix.model;

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

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;

/**
 * Wrapper for a monitoring config file
 */
public class MonitoringConfig extends HelixProperty {
  private static final Logger LOG = Logger.getLogger(MonitoringConfig.class);

  /**
   * Properties describing the config
   */
  public enum MonitoringConfigProperty {
    CONFIG_FILE
  }

  /**
   * Instantiate from a record
   * @param record populated ZNRecord
   */
  public MonitoringConfig(ZNRecord record) {
    super(record);
  }

  /**
   * Create an empty config with a name
   * @param configName the name, will map to a file name
   */
  public MonitoringConfig(String configName) {
    super(configName);
  }

  /**
   * Set the config from an input stream
   * @param is an open InputStream
   */
  public void setConfig(InputStream is) {
    try {
      String config = IOUtils.toString(is);
      setConfig(config);
    } catch (IOException e) {
      LOG.error("Could not persist the monitoring config!");
    }
  }

  /**
   * Set the config from a String
   * @param config a String containing the entire config
   */
  public void setConfig(String config) {
    _record.setSimpleField(MonitoringConfigProperty.CONFIG_FILE.toString(), config);
  }

  /**
   * Get the config
   * @return String containing the config
   */
  public String getConfig() {
    return _record.getSimpleField(MonitoringConfigProperty.CONFIG_FILE.toString());
  }
}
