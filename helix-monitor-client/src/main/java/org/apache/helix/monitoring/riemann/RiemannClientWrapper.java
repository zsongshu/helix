package org.apache.helix.monitoring.riemann;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.helix.monitoring.MonitoringClient;
import org.apache.helix.monitoring.MonitoringEvent;
import org.apache.log4j.Logger;

/**
 * Wrapper around a list of RawRiemannClients; if one client fails, try the next one in list
 */
public class RiemannClientWrapper implements MonitoringClient {
  private static final Logger LOG = Logger.getLogger(RiemannClientWrapper.class);

  /**
   * A list of "host:port" addresses for Riemann servers
   */
  private final List<String> _rsHosts;
  private boolean _isConnected;
  private List<RawRiemannClient> _rclients;
  private int _batchSize;

  private ScheduledThreadPoolExecutor _pool;

  public RiemannClientWrapper(List<String> rsHosts) {
    this(rsHosts, 1);
  }

  public RiemannClientWrapper(List<String> rsHosts, int batchSize) {
    _rsHosts = rsHosts;
    Collections.sort(_rsHosts);
    _batchSize = batchSize > 0 ? batchSize : 1;
    _isConnected = false;
  }

  // Returns the pool for this client. Creates the pool on first use
  private synchronized ScheduledThreadPoolExecutor pool() {
    if (_pool == null) {
      _pool = new ScheduledThreadPoolExecutor(1);
      _pool.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
      _pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    return _pool;
  }

  @Override
  public synchronized void connect() throws Exception {
    if (_isConnected) {
      LOG.warn("Client already connected");
      return;
    }

    _rclients = new ArrayList<RawRiemannClient>();
    for (String rsHost : _rsHosts) {
      String[] splits = rsHost.split(":");

      if (splits == null || splits.length != 2) {
        throw new IllegalArgumentException("Invalid Riemann server: " + rsHost);
      }

      String host = splits[0];
      int port = Integer.parseInt(splits[1]);

      RawRiemannClient rclient = new RawRiemannClient(host, port, _batchSize);
      rclient.connect();

      /**
       * If any Riemann client doesn't support batch, set it to 1
       */
      if (rclient.isConnected() && rclient.getBatchSize() == 1) {
        _batchSize = 1;
      }
      _rclients.add(rclient);
    }

    _isConnected = true;
  }

  @Override
  public synchronized void disconnect() {
    if (!_isConnected || _rclients == null) {
      LOG.warn("Client already disconnected");
      return;
    }

    for (RawRiemannClient rclient : _rclients) {
      rclient.disconnect();
    }
    _rclients = null;
    _isConnected = false;
  }

  /**
   * Get raw client based on event's sharding key
   * @param event
   * @return
   */
  private RawRiemannClient client(MonitoringEvent event) {
    String shardingKey = event.shardingKey();
    int baseIdx = shardingKey.hashCode() % _rsHosts.size();

    // find the first rclient in CONNECTED state and send
    for (int i = 0; i < _rsHosts.size(); i++) {
      int idx = (baseIdx + i) % _rsHosts.size();
      RawRiemannClient rclient = _rclients.get(idx);
      if (rclient.isConnected()) {
        return rclient;
      }
    }
    return null;
  }

  @Override
  public boolean send(MonitoringEvent event) {
    if (!_isConnected || _rclients == null) {
      LOG.warn("Client is not connected. Fail to send event: " + event);
      return false;
    }

    RawRiemannClient rclient = client(event);
    if (rclient != null) {
      return rclient.send(event);
    }

    LOG.error("Fail to send event: " + event + ", no rclient is available");
    return false;
  }

  @Override
  public void every(long interval, long delay, TimeUnit unit, Runnable r) {
    pool().scheduleAtFixedRate(r, delay, interval, unit);
  }

  @Override
  public boolean sendAndFlush(MonitoringEvent event) {
    if (!_isConnected || _rclients == null) {
      LOG.warn("Client is not connected. Fail to send event: " + event);
      return false;
    }

    RawRiemannClient rclient = client(event);
    if (rclient != null) {
      boolean success = rclient.send(event);
      if (success) {
        return rclient.flush();
      }
    }
    LOG.error("Fail to send event: " + event + ", no rclient is available");
    return false;
  }

  @Override
  public boolean isConnected() {
    return _isConnected;
  }

  @Override
  public boolean isBatchingEnabled() {
    if (!_isConnected || _rclients == null) {
      LOG.warn("Client is not connected");
      return false;
    }

    /**
     * Batch should be enabled for all or none raw clients
     */
    for (RawRiemannClient rclient : _rclients) {
      if (rclient.isConnected()) {
        return rclient.isBatchEnabled();
      }
    }
    return false;
  }

  /**
   * Return batch size if connected or 1 otherwise
   */
  @Override
  public int getBatchSize() {
    if (!_isConnected || _rclients == null) {
      LOG.warn("Client is not connected");
      return 1;
    }

    /**
     * Batch size should be the same for all raw clients
     */
    for (RawRiemannClient rclient : _rclients) {
      if (rclient.isConnected()) {
        return rclient.getBatchSize();
      }
    }

    return 1;
  }

  @Override
  public boolean flush() {
    if (!_isConnected || _rclients == null) {
      LOG.warn("Client is not connected");
      return false;
    }

    boolean success = true;
    for (RawRiemannClient rclient : _rclients) {
      success &= rclient.flush();
    }
    return success;
  }
}
