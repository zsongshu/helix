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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.helix.monitoring.MonitoringEvent;
import org.apache.log4j.Logger;

import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.RiemannBatchClient;
import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.UnsupportedJVMException;

/**
 * Simple wrapper around RiemannClient that does auto reconnect
 */
class RawRiemannClient {
  private static final Logger LOG = Logger.getLogger(RawRiemannClient.class);
  private static final int HEARTBEAT_PERIOD = 10;
  private static final int TIMEOUT_LIMIT = 3;

  enum State {
    DISCONNECTED,
    CONNECTED,
    RECONNECTING
  }

  private RiemannClient _rclient;
  private RiemannBatchClient _brclient;
  private volatile State _state = State.DISCONNECTED;
  private final String _host;
  private final int _port;
  private int _batchSize;
  private Thread _reconnectThread;

  public RawRiemannClient(String host, int port) {
    this(host, port, 1);
  }

  public RawRiemannClient(String host, int port, int batchSize) {
    _host = host;
    _port = port;
    _batchSize = batchSize > 0 ? batchSize : 1;
  }

  private synchronized boolean doConnect() {
    if (_state == State.CONNECTED) {
      return true;
    }

    try {
      _rclient = RiemannClient.tcp(_host, _port);
      _rclient.connect();
      if (_rclient != null && _batchSize > 1) {
        try {
          _brclient = new RiemannBatchClient(_batchSize, _rclient);
        } catch (UnknownHostException e) {
          _batchSize = 1;
          LOG.error("Could not resolve host", e);
        } catch (UnsupportedJVMException e) {
          _batchSize = 1;
          LOG.warn("Batching not enabled because of incompatible JVM", e);
        }
      }

      Random random = new Random();
      _rclient.every(HEARTBEAT_PERIOD, random.nextInt(HEARTBEAT_PERIOD), TimeUnit.SECONDS,
          new Runnable() {

            @Override
            public void run() {
              try {
                _rclient.event().service("heartbeat").ttl(TIMEOUT_LIMIT * HEARTBEAT_PERIOD)
                    .sendWithAck();
                _state = State.CONNECTED;
              } catch (Exception e) {
                LOG.error("Exception in send heatbeat to riemann server: " + _host + ":" + _port, e);
                _state = State.RECONNECTING;
              }
            }
          });
      _state = State.CONNECTED;
      return true;
    } catch (IOException e) {
      LOG.error("Fail to connect to riemann server: " + _host + ":" + _port);
    }

    return false;
  }

  /**
   * Make a connection to Riemann server; if fails, start a thread for retrying
   */
  public synchronized void connect() {
    boolean success = doConnect();
    if (!success) {
      _reconnectThread = new Thread(new Runnable() {

        @Override
        public void run() {
          LOG.info("Start reconnect thread");
          Random random = new Random();
          try {
            while (!Thread.currentThread().isInterrupted()) {
              boolean success = doConnect();
              if (success) {
                break;
              }

              TimeUnit.SECONDS.sleep(HEARTBEAT_PERIOD + random.nextInt() % HEARTBEAT_PERIOD);

            }
          } catch (InterruptedException e) {
            LOG.info("Reconnect thread is interrupted");
          } finally {
            LOG.info("Terminate reconnect thread");
          }
        }
      });

      _reconnectThread.start();
    }
  }

  /**
   * Disconnect from Riemann server
   */
  public synchronized void disconnect() {
    try {
      if (_reconnectThread != null) {
        _reconnectThread.interrupt();
      }

      if (_rclient != null) {
        _rclient.scheduler().shutdown();
        _rclient.disconnect();
      }
    } catch (IOException e) {
      LOG.error("Fail to disconnect rclient for " + _host + ":" + _port, e);
    }
    _state = State.DISCONNECTED;
  }

  public boolean isConnected() {
    return _state == State.CONNECTED;
  }

  private AbstractRiemannClient client() {
    if (isBatchEnabled()) {
      return _brclient;
    } else {
      return _rclient;
    }
  }

  public boolean send(MonitoringEvent event) {
    if (!isConnected()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fail to send event: " + event + " to " + _host + ":" + _port
            + ", because state is not connected, was: " + _state);
      }
      return false;
    }

    try {
      ClientUtil.convertEvent(client(), event).send();
      return true;
    } catch (Exception e) {
      LOG.error("Fail to send event: " + event + " to " + _host + ":" + _port, e);
    }
    return false;
  }

  public boolean flush() {
    if (!isConnected()) {
      return false;
    }

    try {
      client().flush();
      return true;
    } catch (IOException e) {
      LOG.error("Problem flushing the Riemann event queue", e);
    }
    return false;
  }

  public boolean sendAndFlush(MonitoringEvent event) {
    boolean success = send(event);
    if (success) {
      return flush();
    }

    return false;
  }

  public int getBatchSize() {
    return _batchSize;
  }

  public boolean isBatchEnabled() {
    return _batchSize > 1;
  }

  public State getState() {
    return _state;
  }
}
