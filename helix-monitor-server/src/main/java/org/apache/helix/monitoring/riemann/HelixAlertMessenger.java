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

import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.alert.AlertName;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.log4j.Logger;

/**
 * Accept alerts from local Riemann server and forward it to helix-controller
 */
public class HelixAlertMessenger {
  private static final Logger LOG = Logger.getLogger(HelixAlertMessenger.class);
  private static final int DEFAULT_MAX_ALERT_COUNT = 1;
  private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MINUTES;

  private final ZkClient _zkclient;
  private final BaseDataAccessor<ZNRecord> _baseAccessor;

  /**
   * A queue that keeps track of timestamps (in millisecond) of last N alerts sent
   */
  private final Queue<Long> _queue;
  private final int _maxAlertCount;
  private final TimeUnit _timeUnit;

  public HelixAlertMessenger(String zkHosts) {
    this(zkHosts, DEFAULT_MAX_ALERT_COUNT, DEFAULT_TIME_UNIT);
  }

  public HelixAlertMessenger(String zkHosts, int maxAlertCount, TimeUnit timeUnit) {
    _zkclient =
        new ZkClient(zkHosts, ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    _baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
    _queue = new LinkedList<Long>();
    _maxAlertCount = maxAlertCount;
    _timeUnit = timeUnit;
  }

  /**
   * Send alert to helix controller; throttle if necessary
   * not thread-safe
   * @param alertNameStr
   */
  public void onAlert(String alertNameStr) {
    LOG.info("Handling alert: " + alertNameStr);

    // throttling
    long now = System.currentTimeMillis();
    if (_queue.size() >= _maxAlertCount) {
      if (_queue.peek() + _timeUnit.toMillis(_maxAlertCount) > now) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Throttling alert: " + alertNameStr);
        }
        return;
      } else {
        _queue.remove();
      }
    }

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

      // record the timestamp
      _queue.add(now);

    } catch (Exception e) {
      LOG.error("Fail to send alert to cluster being monitored: " + alertNameStr, e);
    }
  }
}
