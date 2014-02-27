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

import org.apache.log4j.Logger;

import clojure.lang.RT;
import clojure.lang.Symbol;

/**
 * A monitoring server implementation that uses Riemann
 */
public class RiemannMonitoringServer implements MonitoringServer {
  private static final Logger LOG = Logger.getLogger(RiemannMonitoringServer.class);

  private volatile boolean _isStarted;
  private final RiemannConfigs _config;

  /**
   * Create a monitoring server
   * @param config
   */
  public RiemannMonitoringServer(RiemannConfigs config) {
    LOG.info("Construct RiemannMonitoringServer with configDir: " + config.getConfigDir());
    _config = config;
    config.persistConfigs();
    _isStarted = false;
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting Riemann server with configDir: " + _config.getConfigDir());

    // start Riemann
    RT.var("clojure.core", "require").invoke(Symbol.intern("riemann.bin"));
    RT.var("clojure.core", "require").invoke(Symbol.intern(RiemannConfigs.DEFAULT_RIEMANN_CONFIG));
    RT.var("riemann.bin", "-main").invoke(_config.getConfigDir());
    _isStarted = true;
  }

  @Override
  public synchronized void stop() {
    if (!_isStarted) {
      LOG.error("Tried to stop Riemann when not started!");
      return;
    }
    LOG.info("Stopping Riemann server");
    RT.var("riemann.config", "stop!").invoke();
    _isStarted = false;
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }
}
