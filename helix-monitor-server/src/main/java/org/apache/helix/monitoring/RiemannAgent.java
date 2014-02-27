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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.log4j.Logger;

import com.aphyr.riemann.client.RiemannClient;

public class RiemannAgent {
  private static final Logger LOG = Logger.getLogger(RiemannAgent.class);

  static final String STATEMODEL_NAME = "OnlineOffline";

  final Random _random;
  final String _zkAddr;
  final String _clusterName;
  final String _instanceName;
  final int _riemannPort;
  final HelixManager _participant;
  final RiemannClient _client;

  public RiemannAgent(String zkAddr, String clusterName, int riemannPort) throws IOException {
    _random = new Random();
    _zkAddr = zkAddr;
    _clusterName = clusterName;
    _instanceName =
        String.format("%s_%d", InetAddress.getLocalHost().getCanonicalHostName(), riemannPort);
    _riemannPort = riemannPort;
    _participant =
        HelixManagerFactory.getZKHelixManager(clusterName, _instanceName, InstanceType.PARTICIPANT,
            zkAddr);
    _client = RiemannClient.tcp("localhost", riemannPort);
  }

  public void start() throws Exception {
    LOG.info("Starting RiemannAgent. zk: " + _zkAddr + ", cluster: " + _clusterName
        + ", instance: " + _instanceName + ", riemannPort: " + _riemannPort);

    // Wait until riemann port is connected
    int timeout = 30 * 1000;
    long startT = System.currentTimeMillis();
    while ((System.currentTimeMillis() - startT) < timeout) {
      try {
        _client.connect();
        break;
      } catch (IOException e) {
        int sleep = _random.nextInt(3000) + 3000;
        LOG.info("Wait " + sleep + "ms for riemann server to come up");
        TimeUnit.MILLISECONDS.sleep(sleep);
      }
    }

    if (!_client.isConnected()) {
      String err =
          "Fail to connect to reimann server on localhost:" + _riemannPort + " in " + timeout
              + "ms";
      LOG.error(err);
      throw new RuntimeException(err);
    }
    LOG.info("RiemannAgent connected to local riemann server on port: " + _riemannPort);

    // Start helix participant
    _participant.getStateMachineEngine().registerStateModelFactory(STATEMODEL_NAME,
        new RiemannAgentStateModelFactory());
    _participant.connect();

    // Monitor riemann server
    _client.every(10, 0, TimeUnit.SECONDS, new Runnable() {

      @Override
      public void run() {
        try {
          // send heartbeat metrics
          _client.event().service("heartbeat").state("running").ttl(20).sendWithAck();
        } catch (Exception e) {
          LOG.error("Exception in send heatbeat to local riemann server, shutdown RiemannAgent: "
              + _instanceName, e);
          shutdown();
        }
      }
    });

  }

  public void shutdown() {
    LOG.info("Shutting down RiemannAgent. zk: " + _zkAddr + ", cluster: " + _clusterName
        + ", instance: " + _instanceName + ", riemannPort: " + _riemannPort);

    try {
      _client.scheduler().shutdown();
      _client.disconnect();
    } catch (IOException e) {
      LOG.error("Exception in disconnect riemann client", e);
    }

    _participant.disconnect();
  }
}
