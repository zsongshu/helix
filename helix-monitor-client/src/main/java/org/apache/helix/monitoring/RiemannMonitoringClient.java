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
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.model.Leader;
import org.apache.log4j.Logger;

import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.RiemannBatchClient;
import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.UnsupportedJVMException;
import com.google.common.collect.Lists;

/**
 * A Riemann-based monitoring client
 * Thread safety note: connect and disconnect are serialized to ensure that there
 * is no attempt to connect or disconnect with an inconsistent state. The send routines are not
 * protected for performance reasons, and so a single send/flush may fail.
 */
public class RiemannMonitoringClient implements MonitoringClient {
  private static final Logger LOG = Logger.getLogger(RiemannMonitoringClient.class);
  private String _host;
  private int _port;
  private int _batchSize;
  private RiemannClient _client;
  private RiemannBatchClient _batchClient;
  private List<ScheduledItem> _scheduledItems;
  private HelixDataAccessor _accessor;
  private IZkDataListener _leaderListener;

  /**
   * Create a non-batched monitoring client
   * @param clusterId the cluster to monitor
   * @param accessor an accessor for the cluster
   */
  public RiemannMonitoringClient(ClusterId clusterId, HelixDataAccessor accessor) {
    this(clusterId, accessor, 1);
  }

  /**
   * Create a monitoring client that supports batching
   * @param clusterId the cluster to monitor
   * @param accessor an accessor for the cluster
   * @param batchSize the number of events in a batch
   */
  public RiemannMonitoringClient(ClusterId clusterId, HelixDataAccessor accessor, int batchSize) {
    _host = null;
    _port = -1;
    _batchSize = batchSize > 0 ? batchSize : 1;
    _client = null;
    _batchClient = null;
    _accessor = accessor;
    _scheduledItems = Lists.newLinkedList();
    _leaderListener = getLeaderListener();
  }

  @Override
  public void connect() {
    if (isConnected()) {
      LOG.error("Already connected to Riemann!");
      return;
    }

    // watch for changes
    changeLeaderSubscription(true);

    // do the connect asynchronously as a tcp establishment could take time
    Leader leader = _accessor.getProperty(_accessor.keyBuilder().controllerLeader());
    doConnectAsync(leader);
  }

  @Override
  public void disconnect() {
    changeLeaderSubscription(false);
    disconnectInternal();
  }

  @Override
  public boolean isConnected() {
    return _client != null && _client.isConnected();
  }

  @Override
  public boolean flush() {
    if (!isConnected()) {
      LOG.error("Tried to flush a Riemann client that is not connected!");
      return false;
    }
    AbstractRiemannClient c = getClient(true);
    try {
      c.flush();
      return true;
    } catch (IOException e) {
      LOG.error("Problem flushing the Riemann event queue!", e);
    }
    return false;
  }

  @Override
  public boolean send(MonitoringEvent event, boolean batch) {
    if (!isConnected()) {
      LOG.error("Riemann connection must be active in order to send an event!");
      return false;
    }
    AbstractRiemannClient c = getClient(batch);
    convertEvent(c, event).send();
    return true;
  }

  @Override
  public boolean sendAndFlush(MonitoringEvent event) {
    boolean sendResult = send(event, true);
    if (sendResult) {
      return flush();
    }
    return false;
  }

  @Override
  public boolean isBatchingEnabled() {
    return _batchClient != null && _batchClient.isConnected();
  }

  @Override
  public int getBatchSize() {
    return _batchSize;
  }

  @Override
  public void every(long interval, long delay, TimeUnit unit, Runnable r) {
    ScheduledItem scheduledItem = new ScheduledItem();
    scheduledItem.interval = interval;
    scheduledItem.delay = delay;
    scheduledItem.unit = unit;
    scheduledItem.r = r;
    _scheduledItems.add(scheduledItem);
    if (isConnected()) {
      getClient().every(interval, delay, unit, r);
    }
  }

  /**
   * Get a raw, non-batched Riemann client.
   * WARNING: do not cache this, as it may be disconnected without notice
   * @return RiemannClient
   */
  private RiemannClient getClient() {
    return _client;
  }

  /**
   * Get a batched Riemann client (if batching is supported)
   * WARNING: do not cache this, as it may be disconnected without notice
   * @return RiemannBatchClient
   */
  private RiemannBatchClient getBatchClient() {
    return _batchClient;
  }

  /**
   * Get a Riemann client
   * WARNING: do not cache this, as it may be disconnected without notice
   * @param batch true if the client is preferred to support batching, false otherwise
   * @return AbstractRiemannClient
   */
  private AbstractRiemannClient getClient(boolean batch) {
    if (batch && isBatchingEnabled()) {
      return getBatchClient();
    } else {
      return getClient();
    }
  }

  /**
   * Based on the contents of the leader node, connect to a Riemann server
   * @param leader node containing host/port
   */
  private void doConnectAsync(final Leader leader) {
    new Thread() {
      @Override
      public void run() {
        synchronized (RiemannMonitoringClient.this) {
          // only connect if the leader is available; otherwise it will be picked up by the callback
          if (leader != null) {
            _host = leader.getMonitoringHost();
            _port = leader.getMonitoringPort();
          }
          // connect if there's a valid host and port
          if (_host != null && _port != -1) {
            connectInternal(_host, _port);
          }
        }
      }
    }.start();
  }

  /**
   * Establishment of a connection to a Riemann server
   * @param host monitoring server hostname
   * @param port monitoring server port
   */
  private synchronized void connectInternal(String host, int port) {
    disconnectInternal();
    try {
      _client = RiemannClient.tcp(host, port);
      _client.connect();
      // we might have to reschedule tasks
      for (ScheduledItem item : _scheduledItems) {
        _client.every(item.interval, item.delay, item.unit, item.r);
      }
    } catch (IOException e) {
      LOG.error("Error establishing a connection!", e);
    }
    if (_client != null && getBatchSize() > 1) {
      try {
        _batchClient = new RiemannBatchClient(_batchSize, _client);
      } catch (UnknownHostException e) {
        _batchSize = 1;
        LOG.error("Could not resolve host", e);
      } catch (UnsupportedJVMException e) {
        _batchSize = 1;
        LOG.warn("Batching not enabled because of incompatible JVM", e);
      }
    }
  }

  /**
   * Teardown of a connection to a Riemann server
   */
  private synchronized void disconnectInternal() {
    try {
      if (_batchClient != null && _batchClient.isConnected()) {
        _batchClient.disconnect();
      } else if (_client != null && _client.isConnected()) {
        _client.disconnect();
      }
    } catch (IOException e) {
      LOG.error("Disconnection error", e);
    }
    _batchClient = null;
    _client = null;
  }

  /**
   * Change the subscription status to the Leader node
   * @param subscribe true to subscribe, false to unsubscribe
   */
  private void changeLeaderSubscription(boolean subscribe) {
    String leaderPath = _accessor.keyBuilder().controllerLeader().getPath();
    BaseDataAccessor<ZNRecord> baseAccessor = _accessor.getBaseDataAccessor();
    if (subscribe) {
      baseAccessor.subscribeDataChanges(leaderPath, _leaderListener);
    } else {
      baseAccessor.unsubscribeDataChanges(leaderPath, _leaderListener);
    }
  }

  /**
   * Get callbacks for when the leader changes
   * @return implemented IZkDataListener
   */
  private IZkDataListener getLeaderListener() {
    return new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception {
        Leader leader = new Leader((ZNRecord) data);
        doConnectAsync(leader);
      }

      @Override
      public void handleDataDeleted(String dataPath) throws Exception {
        disconnectInternal();
      }
    };
  }

  /**
   * Change a helix event into a Riemann event
   * @param c Riemann client
   * @param helixEvent helix event
   * @return Riemann EventDSL
   */
  private EventDSL convertEvent(AbstractRiemannClient c, MonitoringEvent helixEvent) {
    EventDSL event = c.event();
    if (helixEvent.host() != null) {
      event = event.host(helixEvent.host());
    }
    if (helixEvent.service() != null) {
      event = event.service(helixEvent.service());
    }
    if (helixEvent.eventState() != null) {
      event = event.state(helixEvent.eventState());
    }
    if (helixEvent.description() != null) {
      event = event.description(helixEvent.description());
    }
    if (helixEvent.time() != null) {
      event = event.time(helixEvent.time());
    }
    if (helixEvent.ttl() != null) {
      event = event.ttl(helixEvent.ttl());
    }
    if (helixEvent.longMetric() != null) {
      event = event.metric(helixEvent.longMetric());
    } else if (helixEvent.floatMetric() != null) {
      event = event.metric(helixEvent.floatMetric());
    } else if (helixEvent.doubleMetric() != null) {
      event = event.metric(helixEvent.doubleMetric());
    }
    if (!helixEvent.tags().isEmpty()) {
      event = event.tags(helixEvent.tags());
    }
    if (!helixEvent.attributes().isEmpty()) {
      event = event.attributes(helixEvent.attributes());
    }
    return event;
  }

  /**
   * Wrapper for a task that should be run to a schedule
   */
  private static class ScheduledItem {
    long interval;
    long delay;
    TimeUnit unit;
    Runnable r;

    @Override
    public boolean equals(Object other) {
      if (other instanceof ScheduledItem) {
        ScheduledItem that = (ScheduledItem) other;
        return interval == that.interval && delay == that.delay && unit == that.unit && r == that.r;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public String toString() {
      return String.format("interval: %d|delay: %d|timeunit: %s|runnable: %s", interval, delay,
          unit.toString(), r.toString());
    }
  }
}
