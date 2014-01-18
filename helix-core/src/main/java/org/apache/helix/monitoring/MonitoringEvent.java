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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SpectatorId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
  private List<String> _tags;
  private Map<String, String> _attributes;

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

  // below are a set of package-private getters for each of the fields

  String host() {
    return _host;
  }

  String service() {
    if (_clusterId == null) {
      _clusterId = ClusterId.from("%");
    }
    if (_resourceId == null) {
      _resourceId = ResourceId.from("%");
    }
    if (_partitionId == null) {
      _partitionId = PartitionId.from("%");
    }
    return String.format("%s|%s|%s|%s", _clusterId, _resourceId, _partitionId, _name);
  }

  String eventState() {
    return _eventState;
  }

  String description() {
    return _description;
  }

  Long time() {
    return _time;
  }

  Long longMetric() {
    return _longMetric;
  }

  Float floatMetric() {
    return _floatMetric;
  }

  Double doubleMetric() {
    return _doubleMetric;
  }

  Float ttl() {
    return _ttl;
  }

  List<String> tags() {
    return _tags;
  }

  Map<String, String> attributes() {
    return _attributes;
  }
}
