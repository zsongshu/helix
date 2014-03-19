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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.Scope;
import org.apache.helix.api.Scope.ScopeType;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SpectatorId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A generic monitoring event based on Helix constructs. This is based on Riemann's EventDSL.
 */
public class MonitoringEvent {
  private ClusterId _clusterId;
  private ResourceId _resourceId;
  private PartitionId _partitionId;
  private String _name;
  private String _host;
  private String _eventState;
  private String _description;
  private Long _time;
  private Long _longMetric;
  private Float _floatMetric;
  private Double _doubleMetric;
  private Float _ttl;
  private final List<String> _tags;
  private final Map<String, String> _attributes;
  private String _shardingStr;
  private final Set<ScopeType> _shardingScopes;

  /**
   * Create an empty MonitoringEvent
   */
  public MonitoringEvent() {
    _clusterId = null;
    _resourceId = null;
    _partitionId = null;
    _name = null;
    _host = null;
    _eventState = null;
    _description = null;
    _time = null;
    _longMetric = null;
    _floatMetric = null;
    _doubleMetric = null;
    _ttl = null;
    _tags = Lists.newLinkedList();
    _attributes = Maps.newHashMap();
    _shardingStr = null;
    _shardingScopes = Sets.newHashSet();
  }

  /**
   * Give this event a name
   * @param name the name
   * @return MonitoringEvent
   */
  public MonitoringEvent name(String name) {
    _name = name;
    return this;
  }

  /**
   * Set the cluster this event corresponds to
   * @param clusterId the cluster id
   * @return MonitoringEvent
   */
  public MonitoringEvent cluster(ClusterId clusterId) {
    _clusterId = clusterId;
    return this;
  }

  /**
   * Set the participant this event corresponds to
   * @param participantId the participant id
   * @return MonitoringEvent
   */
  public MonitoringEvent participant(ParticipantId participantId) {
    _host = participantId.stringify();
    return this;
  }

  /**
   * Set the spectator this event corresponds to
   * @param spectatorId the spectator id
   * @return MonitoringEvent
   */
  public MonitoringEvent spectator(SpectatorId spectatorId) {
    _host = spectatorId.stringify();
    return this;
  }

  /**
   * Set the controller this event corresponds to
   * @param controllerId the controller id
   * @return MonitoringEvent
   */
  public MonitoringEvent controller(ControllerId controllerId) {
    _host = controllerId.stringify();
    return this;
  }

  /**
   * Set the resource this event corresponds to
   * @param resourceId the resource id
   * @return MonitoringEvent
   */
  public MonitoringEvent resource(ResourceId resourceId) {
    _resourceId = resourceId;
    return this;
  }

  /**
   * Set the partition this event corresponds to
   * @param partitionId the partition id
   * @return MonitoringEvent
   */
  public MonitoringEvent partition(PartitionId partitionId) {
    _partitionId = partitionId;
    return this;
  }

  /**
   * Set the state of the metric
   * @param eventState the event state (e.g. "OK", "Failing", etc)
   * @return MonitoringEvent
   */
  public MonitoringEvent eventState(String eventState) {
    _eventState = eventState;
    return this;
  }

  /**
   * Give the event a description
   * @param description descriptive text
   * @return MonitoringEvent
   */
  public MonitoringEvent description(String description) {
    _description = description;
    return this;
  }

  /**
   * Set the time that the event occurred
   * @param time long UNIX timestamp
   * @return MonitoringEvent
   */
  public MonitoringEvent time(long time) {
    _time = time;
    return this;
  }

  /**
   * Give the event a long metric
   * @param metric the metric (the measured quantity)
   * @return MonitoringEvent
   */
  public MonitoringEvent metric(long metric) {
    _longMetric = metric;
    return this;
  }

  /**
   * Give the event a float metric
   * @param metric the metric (the measured quantity)
   * @return MonitoringEvent
   */
  public MonitoringEvent metric(float metric) {
    _floatMetric = metric;
    return this;
  }

  /**
   * Give the event a double metric
   * @param metric the metric (the measured quantity)
   * @return MonitoringEvent
   */
  public MonitoringEvent metric(double metric) {
    _doubleMetric = metric;
    return this;
  }

  /**
   * Give the time before the event will expire
   * @param ttl time to live
   * @return MonitoringEvent
   */
  public MonitoringEvent ttl(float ttl) {
    _ttl = ttl;
    return this;
  }

  /**
   * Add a tag to the event
   * @param tag arbitrary string
   * @return MonitoringEvent
   */
  public MonitoringEvent tag(String tag) {
    _tags.add(tag);
    return this;
  }

  /**
   * Add multiple tags to an event
   * @param tags a collection of tags
   * @return MonitoringEvent
   */
  public MonitoringEvent tags(Collection<String> tags) {
    _tags.addAll(tags);
    return this;
  }

  /**
   * Add an attribute (a key-value pair)
   * @param name the attribute name
   * @param value the attribute value
   * @return MonitoringEvent
   */
  public MonitoringEvent attribute(String name, String value) {
    _attributes.put(name, value);
    return this;
  }

  /**
   * Add multiple attributes
   * @param attributes map of attribute name to value
   * @return MonitoringEvent
   */
  public MonitoringEvent attributes(Map<String, String> attributes) {
    _attributes.putAll(attributes);
    return this;
  }

  /**
   * Set sharding key using string
   * @param shardingStr
   * @return MonitoringEvent
   */
  public MonitoringEvent shardingString(String shardingStr) {
    _shardingStr = shardingStr;
    return this;
  }

  /**
   * Set sharding key using scopes
   * @param scopes
   * @return MonitoringEvent
   */
  public MonitoringEvent shardingScopes(ScopeType... scopes) {
    _shardingScopes.clear();
    _shardingScopes.addAll(Arrays.asList(scopes));
    return this;
  }

  /**
   * Return sharding key which is used by MonitoringClient to choose MonitoringServer
   * @return sharding key
   */
  public String shardingKey() {
    // if shardingStr exists, use shardingStr
    if (_shardingStr != null) {
      return _shardingStr;
    }

    // if shardingStr doesn't exist, use shardingScopes
    if (_shardingScopes.isEmpty()) {
      _shardingScopes.addAll(Arrays.asList(ScopeType.CLUSTER, ScopeType.RESOURCE));
    }

    StringBuilder sb = new StringBuilder();
    if (_shardingScopes.contains(ScopeType.CLUSTER)) {
      sb.append(_clusterId == null ? "%" : _clusterId.stringify());
    }
    if (_shardingScopes.contains(ScopeType.RESOURCE)) {
      sb.append("|");
      sb.append(_resourceId == null ? "%" : _resourceId.stringify());
    }
    if (_shardingScopes.contains(ScopeType.PARTITION)) {
      sb.append("|");
      sb.append(_partitionId == null ? "%" : _partitionId.stringify());
    }
    if (_shardingScopes.contains(ScopeType.PARTICIPANT)) {
      sb.append("|");
      sb.append(_host == null ? "%" : _host);
    }

    return sb.toString();
  }

  // below are used for converting MonitoringEvent to Riemann EventDSL

  public String host() {
    return _host;
  }

  public String service() {
    if (_clusterId == null) {
      _clusterId = ClusterId.from("%");
    }
    if (_resourceId == null) {
      _resourceId = ResourceId.from("%");
    }
    if (_partitionId == null) {
      _partitionId = PartitionId.from("%");
    }
    if (_name == null) {
      _name = "%";
    }
    return String.format("%s|%s|%s|%s", _clusterId, _resourceId, _partitionId, _name);
  }

  public String eventState() {
    return _eventState;
  }

  public String description() {
    return _description;
  }

  public Long time() {
    return _time;
  }

  public Long longMetric() {
    return _longMetric;
  }

  public Float floatMetric() {
    return _floatMetric;
  }

  public Double doubleMetric() {
    return _doubleMetric;
  }

  public Float ttl() {
    return _ttl;
  }

  public List<String> tags() {
    return _tags;
  }

  public Map<String, String> attributes() {
    return _attributes;
  }
}
