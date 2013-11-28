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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.MonitoringConfig;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;

import clojure.lang.RT;
import clojure.lang.Symbol;

/**
 * A monitoring server implementation that uses Riemann
 */
public class RiemannMonitoringServer implements MonitoringServer {
  private static final Logger LOG = Logger.getLogger(RiemannMonitoringServer.class);
  private static final String DEFAULT_CONFIG_DIR = "/tmp/riemannconfigs";
  private final String _host;
  private boolean _isStarted;
  private boolean _configsAdded;
  private String _configDir;

  /**
   * Create a monitoring server. Configs will be placed in "/tmp/riemannconfigs".
   */
  public RiemannMonitoringServer(String host) {
    this(DEFAULT_CONFIG_DIR, host);
  }

  /**
   * Create a monitoring server.
   * @param configDir Directory to use for storing configs
   * @param host Hostname where the server lives (i.e. the hostname of this machine)
   */
  public RiemannMonitoringServer(String configDir, String host) {
    _configDir = configDir;
    _isStarted = false;
    _configsAdded = false;
    _host = host;
  }

  @Override
  public synchronized void start() {
    // get the config file
    URL url = Thread.currentThread().getContextClassLoader().getResource("riemann.config");
    if (url == null) {
      LOG.error("Riemann config file does not exist!");
      return;
    }
    String path = url.getPath();
    LOG.info("Riemann config file is at: " + path);

    // register config files
    if (_configsAdded) {
      try {
        File srcFile = new File(path);
        File file = File.createTempFile("riemann", "config");
        file.deleteOnExit();
        FileUtils.copyFile(srcFile, file);
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
        out.println("\n(include \"" + _configDir + "\")\n");
        out.close();
        path = file.getAbsolutePath();
      } catch (IOException e) {
        LOG.error("Could not add configs!");
      }
    }

    // start Riemann
    RT.var("clojure.core", "require").invoke(Symbol.intern("riemann.bin"));
    RT.var("clojure.core", "require").invoke(Symbol.intern("riemann.config"));
    RT.var("riemann.bin", "-main").invoke(path);
    _isStarted = true;
  }

  @Override
  public synchronized void stop() {
    if (!_isStarted) {
      LOG.error("Tried to stop Riemann when not started!");
      return;
    }
    RT.var("riemann.config", "stop!").invoke();
    _isStarted = false;
  }

  @Override
  public synchronized void addConfigs(HelixDataAccessor accessor) {
    // create the directory
    File dir = new File(_configDir);
    if (!dir.exists()) {
      dir.mkdir();
    }

    // persist ZK-based configs
    if (accessor != null) {
      List<MonitoringConfig> configs =
          accessor.getChildValues(accessor.keyBuilder().monitoringConfigs());
      for (MonitoringConfig config : configs) {
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

    // restart if started
    if (_isStarted) {
      stop();
      start();
    }
    _configsAdded = true;
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  @Override
  public String getHost() {
    return _host;
  }

  @Override
  public int getPort() {
    return 5555;
  }

  public static void main(String[] args) throws InterruptedException, UnknownHostException {
    RiemannMonitoringServer service =
        new RiemannMonitoringServer(InetAddress.getLocalHost().getHostName());
    BaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(ZKClientPool.getZkClient("eat1-app87.corp:2181"));
    HelixDataAccessor accessor = new ZKHelixDataAccessor("perf-test-cluster", baseAccessor);
    service.addConfigs(accessor);
    service.start();
    Thread.currentThread().join();
  }
}
