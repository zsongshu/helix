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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.log4j.Logger;

import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.RiemannBatchClient;
import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.UnsupportedJVMException;
import com.google.common.collect.Lists;

/**
 * A Riemann-based monitoring client Thread safety note: connect and disconnect are serialized to
 * ensure that there is no attempt to connect or disconnect with an inconsistent state. The send
 * routines are not protected for performance reasons, and so a single send/flush may fail.
 */
public class RiemannMonitoringClient implements MonitoringClient {
  private static final Logger LOG = Logger.getLogger(RiemannMonitoringClient.class);
  public static final String DEFAULT_MONITORING_SERVICE_NAME = "MonitoringService";

  /**
   * Contains information about a RiemannClient inside a MonitoringClient
   */
  class MonitoringClientInfo {
    /**
     * host/port of riemann server to which this client connects
     */
    String _host;
    int _port;

    /**
     * riemann client
     */
    RiemannClient _client;

    /**
     * batch rieman client, null if batch is not enabled
     */
    RiemannBatchClient _batchClient;

    /**
     * list of periodic tasks scheduled on this riemann client
     */
    final List<ScheduledItem> _scheduledItems;

    public MonitoringClientInfo() {
      _host = null;
      _port = -1;
      _client = null;
      _batchClient = null;
      _scheduledItems = Lists.newArrayList();
    }

  }

  private int _batchSize;
  private final ResourceId _monitoringServiceName;
  private int _monitoringServicePartitionNum;

  private final HelixManager _spectator;
  private final RoutingTableProvider _routingTableProvider;
  private final Map<ResourceId, MonitoringClientInfo> _clientMap;

  /**
   * Create a non-batched monitoring client
   * @param zkAddr
   * @param monitoringClusterId
   */
  public RiemannMonitoringClient(String zkAddr, ClusterId monitoringClusterId) {
    this(zkAddr, monitoringClusterId, ResourceId.from(DEFAULT_MONITORING_SERVICE_NAME), 1);
  }

  /**
   * Create a monitoring client that supports batching
   * @param clusterId
   *          the cluster to monitor
   * @param accessor
   *          an accessor for the cluster
   * @param batchSize
   *          the number of events in a batch
   * @throws Exception
   */
  public RiemannMonitoringClient(String zkAddr, ClusterId monitoringClusterId,
      ResourceId monitoringServiceName, int batchSize) {
    _batchSize = batchSize > 0 ? batchSize : 1;
    _monitoringServiceName = monitoringServiceName;
    _monitoringServicePartitionNum = 0;
    _clientMap = new ConcurrentHashMap<ResourceId, RiemannMonitoringClient.MonitoringClientInfo>();

    _spectator =
        HelixManagerFactory.getZKHelixManager(monitoringClusterId.stringify(), null,
            InstanceType.SPECTATOR, zkAddr);
    _routingTableProvider = new RoutingTableProvider();
  }

  @Override
  public void connect() throws Exception {
    if (isConnected()) {
      LOG.error("Already connected to Riemann!");
      return;
    }

    // Connect spectator to the cluster being monitored
    _spectator.connect();
    _spectator.addExternalViewChangeListener(_routingTableProvider);

    // Get partition number of monitoring service
    HelixDataAccessor accessor = _spectator.getHelixDataAccessor();
    IdealState idealState =
        accessor.getProperty(accessor.keyBuilder().idealStates(_monitoringServiceName.stringify()));
    _monitoringServicePartitionNum = idealState.getNumPartitions();
  }

  @Override
  public void disconnect() {
    // disconnect internal riemann clients
    for (ResourceId resource : _clientMap.keySet()) {
      disconnectInternal(resource);
    }

    _spectator.disconnect();
    _monitoringServicePartitionNum = 0;
  }

  @Override
  public boolean isConnected() {
    return _spectator.isConnected();
  }

  /**
   * Flush a riemann client for a resource
   * @param resource
   * @return
   */
  private boolean flush(ResourceId resource) {
    if (!isConnected()) {
      LOG.error("Tried to flush a Riemann client that is not connected!");
      return false;
    }

    AbstractRiemannClient c = getClient(resource, true);
    if (c == null) {
      LOG.warn("Fail to get riemann client for resource: " + resource);
      return false;
    }

    try {
      c.flush();
      return true;
    } catch (IOException e) {
      LOG.error("Problem flushing the Riemann event queue for resource: " + resource, e);
    }
    return false;
  }

  @Override
  public boolean flush() {
    boolean succeed = true;
    for (ResourceId resource : _clientMap.keySet()) {
      succeed = succeed && flush(resource);
    }

    return succeed;
  }

  @Override
  public boolean send(ResourceId resource, MonitoringEvent event, boolean batch) {
    if (!isConnected()) {
      LOG.error("Riemann connection must be active in order to send an event!");
      return false;
    }

    if (!isConnected(resource)) {
      connect(resource, null, event);
    } else {
      AbstractRiemannClient c = getClient(resource, batch);
      convertEvent(c, event).send();
    }

    return true;
  }

  @Override
  public boolean sendAndFlush(ResourceId resource, MonitoringEvent event) {

    boolean sendResult = send(resource, event, true);
    if (sendResult) {
      return flush(resource);
    }
    return false;
  }

  /**
   * Batch should be enabled for either all or none of riemann clients
   */
  @Override
  public boolean isBatchingEnabled() {
    return _batchSize > 1;
  }

  @Override
  public int getBatchSize() {
    return _batchSize;
  }

  /**
   * Check if a riemann client for given resource is connected
   * @param resource
   * @return true if riemann client is connected, false otherwise
   */
  private boolean isConnected(ResourceId resource) {
    if (!isConnected()) {
      return false;
    }

    MonitoringClientInfo clientInfo = _clientMap.get(resource);
    return clientInfo != null && clientInfo._client != null && clientInfo._client.isConnected();
  }

  @Override
  public synchronized void every(ResourceId resource, long interval, long delay, TimeUnit unit,
      Runnable r) {
    if (!isConnected()) {
      LOG.error("Riemann client must be connected in order to send events!");
      return;
    }

    ScheduledItem scheduledItem = new ScheduledItem();
    scheduledItem.interval = interval;
    scheduledItem.delay = delay;
    scheduledItem.unit = unit;
    scheduledItem.r = r;

    if (isConnected(resource)) {
      MonitoringClientInfo clientInfo = _clientMap.get(resource);
      clientInfo._scheduledItems.add(scheduledItem);
      getClient(resource).every(interval, delay, unit, r);
    } else {
      connect(resource, scheduledItem, null);
    }
  }

  /**
   * Connect a riemann client to riemann server given a resource
   * @param resource
   * @param scheduledItem
   * @param pendingEvent
   */
  private void connect(ResourceId resource, ScheduledItem scheduledItem,
      MonitoringEvent pendingEvent) {
    // Hash by resourceId
    int partitionKey = resource.hashCode() % _monitoringServicePartitionNum;
    List<InstanceConfig> instances =
        _routingTableProvider.getInstances(_monitoringServiceName.stringify(),
            _monitoringServiceName + "_" + partitionKey, "ONLINE");

    if (instances.size() == 0) {
      LOG.error("Riemann monitoring server for resource: " + resource + " at partitionKey: "
          + partitionKey + " is not available");
      return;
    }

    InstanceConfig instanceConfig = instances.get(0);
    String host = instanceConfig.getHostName();
    int port = Integer.parseInt(instanceConfig.getPort());

    // Do the connect asynchronously as a tcp establishment could take time
    doConnectAsync(resource, host, port, scheduledItem, pendingEvent);
  }

  /**
   * Get a raw, non-batched Riemann client. WARNING: do not cache this, as it may be disconnected
   * without notice
   * @return RiemannClient
   */
  private RiemannClient getClient(ResourceId resource) {
    MonitoringClientInfo clientInfo = _clientMap.get(resource);
    return clientInfo == null ? null : clientInfo._client;
  }

  /**
   * Get a batched Riemann client (if batching is supported) WARNING: do not cache this, as it may
   * be disconnected without notice
   * @return RiemannBatchClient
   */
  private RiemannBatchClient getBatchClient(ResourceId resource) {
    MonitoringClientInfo clientInfo = _clientMap.get(resource);
    return clientInfo == null ? null : clientInfo._batchClient;
  }

  /**
   * Get a Riemann client WARNING: do not cache this, as it may be disconnected without notice
   * @param batch
   *          true if the client is preferred to support batching, false otherwise
   * @return AbstractRiemannClient
   */
  private AbstractRiemannClient getClient(ResourceId resource, boolean batch) {
    if (batch && isBatchingEnabled()) {
      return getBatchClient(resource);
    } else {
      return getClient(resource);
    }
  }

  /**
   * Based on the contents of the leader node, connect to a Riemann server
   * @param leader
   *          node containing host/port
   */
  private void doConnectAsync(final ResourceId resource, final String host, final int port,
      final ScheduledItem scheduledItem, final MonitoringEvent pendingEvent) {
    new Thread() {
      @Override
      public void run() {
        synchronized (RiemannMonitoringClient.this) {
          if (resource != null && host != null && port != -1) {
            connectInternal(resource, host, port, scheduledItem, pendingEvent);
          } else {
            LOG.error("Fail to doConnectAsync becaue of invalid arguments, resource: " + resource
                + ", host: " + host + ", port: " + port);
          }
        }
      }
    }.start();
  }

  /**
   * Establishment of a connection to a Riemann server
   * @param resource
   * @param host
   * @param port
   * @param scheduledItem
   * @param pendingEvent
   */
  private synchronized void connectInternal(ResourceId resource, String host, int port,
      ScheduledItem scheduledItem, MonitoringEvent pendingEvent) {
    MonitoringClientInfo clientInfo = _clientMap.get(resource);
    if (clientInfo != null && clientInfo._host.equals(host) && clientInfo._port == port
        && clientInfo._client != null && clientInfo._client.isConnected()) {
      LOG.info("Riemann client for resource: " + resource + " already connected on " + host + ":"
          + port);

      // We might have to reschedule tasks
      if (scheduledItem != null) {
        clientInfo._scheduledItems.add(scheduledItem);
        clientInfo._client.every(scheduledItem.interval, scheduledItem.delay, scheduledItem.unit,
            scheduledItem.r);
      }

      // Sending over pending event
      if (pendingEvent != null) {
        convertEvent(clientInfo._client, pendingEvent).send();
      }

      return;
    }

    // Disconnect from previous riemann server
    disconnectInternal(resource);

    // Connect to new riemann server
    RiemannClient client = null;
    RiemannBatchClient batchClient = null;
    try {
      client = RiemannClient.tcp(host, port);
      client.connect();
    } catch (IOException e) {
      LOG.error("Error establishing a connection!", e);

    }

    if (client != null && getBatchSize() > 1) {
      try {
        batchClient = new RiemannBatchClient(_batchSize, client);
      } catch (UnknownHostException e) {
        _batchSize = 1;
        LOG.error("Could not resolve host", e);
      } catch (UnsupportedJVMException e) {
        _batchSize = 1;
        LOG.warn("Batching not enabled because of incompatible JVM", e);
      }
    }

    if (clientInfo == null) {
      clientInfo = new MonitoringClientInfo();
    }

    clientInfo._host = host;
    clientInfo._port = port;
    clientInfo._client = client;
    clientInfo._batchClient = batchClient;
    if (scheduledItem != null) {
      clientInfo._scheduledItems.add(scheduledItem);
    }
    _clientMap.put(resource, clientInfo);

    // We might have to reschedule tasks
    for (ScheduledItem item : clientInfo._scheduledItems) {
      client.every(item.interval, item.delay, item.unit, item.r);
    }

    // Send over pending event
    if (pendingEvent != null) {
      convertEvent(client, pendingEvent).send();
    }
  }

  /**
   * Teardown of a connection to a Riemann server
   */
  private synchronized void disconnectInternal(ResourceId resource) {
    MonitoringClientInfo clientInfo = _clientMap.get(resource);
    if (clientInfo == null) {
      return;
    }

    RiemannBatchClient batchClient = clientInfo._batchClient;
    RiemannClient client = clientInfo._client;

    clientInfo._batchClient = null;
    clientInfo._client = null;

    try {
      if (batchClient != null && batchClient.isConnected()) {
        batchClient.scheduler().shutdown();
        batchClient.disconnect();
      } else if (client != null && client.isConnected()) {
        client.scheduler().shutdown();
        client.disconnect();
      }
    } catch (IOException e) {
      LOG.error("Disconnection error", e);
    }
  }

  /**
   * Change a helix monitoring event into a Riemann event
   * @param c Riemann client
   * @param helixEvent helix event
   * @return Riemann EventDSL
   */
  private EventDSL convertEvent(AbstractRiemannClient c, MonitoringEvent helixEvent) {
    EventDSL event = c.event();
    if (helixEvent.host() != null) {
      event.host(helixEvent.host());
    }
    if (helixEvent.service() != null) {
      event.service(helixEvent.service());
    }
    if (helixEvent.eventState() != null) {
      event.state(helixEvent.eventState());
    }
    if (helixEvent.description() != null) {
      event.description(helixEvent.description());
    }
    if (helixEvent.time() != null) {
      event.time(helixEvent.time());
    }
    if (helixEvent.ttl() != null) {
      event.ttl(helixEvent.ttl());
    }
    if (helixEvent.longMetric() != null) {
      event.metric(helixEvent.longMetric());
    } else if (helixEvent.floatMetric() != null) {
      event.metric(helixEvent.floatMetric());
    } else if (helixEvent.doubleMetric() != null) {
      event.metric(helixEvent.doubleMetric());
    }
    if (!helixEvent.tags().isEmpty()) {
      event.tags(helixEvent.tags());
    }
    if (!helixEvent.attributes().isEmpty()) {
      event.attributes.putAll(helixEvent.attributes());
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
