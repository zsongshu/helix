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
import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.log4j.Logger;

import com.aphyr.riemann.client.RiemannClient;

/**
 * Start a Helix participant that joins cluster and represents local Riemann server
 */
public class RiemannAgent {
  private static final Logger LOG = Logger.getLogger(RiemannAgent.class);
  private static final int HEARTBEAT_PERIOD = 10;
  private static final int TIMEOUT_LIMIT = 3;

  private final String _zkAddr;
  private final String _clusterName;
  private final String _instanceName;
  private final int _riemannPort;
  private HelixManager _participant;
  private final RiemannClient _client;
  private Thread _reconnectThread;

  public RiemannAgent(String zkAddr, String clusterName, int riemannPort) throws IOException {
    _zkAddr = zkAddr;
    _clusterName = clusterName;
    _instanceName =
        String.format("%s_%d", InetAddress.getLocalHost().getCanonicalHostName(), riemannPort);
    _riemannPort = riemannPort;
    _client = RiemannClient.tcp("localhost", riemannPort);
  }

  private synchronized boolean doStart() throws Exception {
    try {
      _client.connect();
      _client.event().service("heartbeat").state("running").ttl(TIMEOUT_LIMIT * HEARTBEAT_PERIOD)
          .sendWithAck();
      LOG.info("RiemannAgent connected to local riemann server on localhost:" + _riemannPort);
      _participant =
          HelixManagerFactory.getZKHelixManager(_clusterName, _instanceName,
              InstanceType.PARTICIPANT, _zkAddr);
      _participant.connect();

      // Monitoring Riemann server
      Random random = new Random();
      _client.every(HEARTBEAT_PERIOD, random.nextInt(HEARTBEAT_PERIOD), TimeUnit.SECONDS,
          new Runnable() {

            @Override
            public void run() {
              try {
                // Send heartbeat metrics
                _client.event().service("heartbeat").state("running")
                    .ttl(TIMEOUT_LIMIT * HEARTBEAT_PERIOD).sendWithAck();
                if (_participant == null) {
                  _participant =
                      HelixManagerFactory.getZKHelixManager(_clusterName, _instanceName,
                          InstanceType.PARTICIPANT, _zkAddr);
                  _participant.connect();
                }
              } catch (Exception e) {
                LOG.error(
                    "Exception in send heatbeat to local riemann server, shutdown RiemannAgent: "
                        + _instanceName, e);

                if (_participant != null) {
                  _participant.disconnect();
                  _participant = null;
                }
              }
            }
          });

      return true;
    } catch (IOException e) {
      LOG.error("Fail to connect to Riemann server on localhost:" + _riemannPort);
    }
    return false;
  }

  /**
   * Try connect local Riemann server; if fails, start a thread to retry async
   * @throws Exception
   */
  public synchronized void start() throws Exception {
    LOG.info("Starting RiemannAgent. zk: " + _zkAddr + ", cluster: " + _clusterName
        + ", instance: " + _instanceName + ", riemannPort: " + _riemannPort);

    boolean success = doStart();
    if (!success) {
      _reconnectThread = new Thread(new Runnable() {

        @Override
        public void run() {
          LOG.info("Start reconnect thread");
          Random random = new Random();
          try {
            while (!Thread.currentThread().isInterrupted()) {
              boolean success = doStart();
              if (success) {
                break;
              }

              TimeUnit.SECONDS.sleep(HEARTBEAT_PERIOD + random.nextInt() % HEARTBEAT_PERIOD);

            }
          } catch (InterruptedException e) {
            LOG.info("Reconnect thread is interrupted");
          } catch (Exception e) {
            LOG.error("Fail to start RiemannAgent", e);
          } finally {
            LOG.info("Terminate reconnect thread");
          }

        }
      });
      _reconnectThread.start();
    }

  }

  public synchronized void shutdown() {
    LOG.info("Shutting down RiemannAgent. zk: " + _zkAddr + ", cluster: " + _clusterName
        + ", instance: " + _instanceName + ", riemannPort: " + _riemannPort);

    if (_reconnectThread != null) {
      _reconnectThread.interrupt();
      _reconnectThread = null;
    }

    try {
      _client.scheduler().shutdown();
      _client.disconnect();
    } catch (IOException e) {
      LOG.error("Exception in disconnect riemann client", e);
    }

    if (_participant != null) {
      _participant.disconnect();
      _participant = null;
    }
  }
}
