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
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.alert.AlertName;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Accept alerts from local riemann server and forward it to helix-controller
 */
public class RiemannAlertProxy {
  private static final Logger LOG = Logger.getLogger(RiemannAlertProxy.class);

  class RiemannAlertProxyHandler extends AbstractHandler {
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {
      // Read content-body
      InputStream inputStream = request.getInputStream();
      StringWriter writer = new StringWriter();
      IOUtils.copy(inputStream, writer, Charset.defaultCharset().toString());
      String alertNameStr = writer.toString();
      LOG.info("Handling alert: " + alertNameStr);

      // Send alert message to the controller of cluster being monitored
      try {
        AlertName alertName = AlertName.from(alertNameStr);
        String clusterName = alertName.getScope().getClusterId().stringify();
        HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();
        Message message = new Message(MessageType.ALERT, UUID.randomUUID().toString());
        message.setAttribute(Message.Attributes.ALERT_NAME, alertNameStr);
        message.setTgtSessionId("*");
        message.setTgtName("controller");
        accessor.setProperty(keyBuilder.controllerMessage(message.getId()), message);
      } catch (Exception e) {
        LOG.error("Fail to send alert to cluster being monitored: " + alertNameStr, e);
      }

      // return ok
      response.setStatus(HttpServletResponse.SC_OK);
      baseRequest.setHandled(true);
    }
  }

  final int _proxyPort;
  final Server _server;
  final BaseDataAccessor<ZNRecord> _baseAccessor;
  final AbstractHandler _handler;

  public RiemannAlertProxy(int proxyPort, BaseDataAccessor<ZNRecord> baseAccessor) {
    _proxyPort = proxyPort;
    _server = new Server(proxyPort);
    _baseAccessor = baseAccessor;
    _handler = new RiemannAlertProxyHandler();
  }

  public void start() throws Exception {
    LOG.info("Starting RiemannAlertProxy on port: " + _proxyPort);
    _server.setHandler(_handler);
    _server.start();

  }

  public void shutdown() {
    try {
      LOG.info("Stopping RiemannAlertProxy on port: " + _proxyPort);
      _server.stop();
    } catch (Exception e) {
      LOG.error("Fail to stop RiemannAlertProxy", e);
    }
  }
}
