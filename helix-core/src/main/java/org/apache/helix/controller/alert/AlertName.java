package org.apache.helix.controller.alert;

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

import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;

public class AlertName {
  final AlertScope _scope;
  final String _metric;

  final AlertComparator _comparator;
  final String _value;

  public static class Builder {
    private ClusterId _clusterId = null;
    private Id _tenantId = null;
    private Id _nodeId = null;
    private ResourceId _resourceId = null;
    private PartitionId _partitionId = null;
    private String _metric = null;
    private AlertComparator _comparator = null;
    private String _value = null;

    public Builder cluster(ClusterId clusterId) {
      _clusterId = clusterId;
      return this;
    }

    public Builder tenant(Id tenantId) {
      _tenantId = tenantId;
      return this;
    }

    public Builder node(Id nodeId) {
      _nodeId = nodeId;
      return this;
    }

    public Builder resource(ResourceId resourceId) {
      _resourceId = resourceId;
      return this;
    }

    public Builder partitionId(PartitionId partitionId) {
      _partitionId = partitionId;
      return this;
    }

    public Builder metric(String metric) {
      _metric = metric;
      return this;
    }

    public Builder largerThan(String value) {
      _comparator = AlertComparator.LARGER;
      _value = value;
      return this;
    }

    public Builder smallerThan(String value) {
      _comparator = AlertComparator.SMALLER;
      _value = value;
      return this;
    }

    public AlertName build() {
      return new AlertName(
          new AlertScope(_clusterId, _tenantId, _nodeId, _resourceId, _partitionId), _metric,
          _comparator, _value);
    }
  }

  public AlertName(AlertScope scope, String metric, AlertComparator comparator, String value) {
    if (scope == null || metric == null || comparator == null || value == null) {
      throw new NullPointerException("null arguments, was scope: " + scope + ", metric: " + metric
          + ", comparator: " + comparator + ", value: " + value);
    }
    _scope = scope;
    _metric = metric;
    _comparator = comparator;
    _value = value;
  }

  /**
   * represent alertName in form of:
   * (scope)(metric)comparator(value)
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(").append(_scope.toString()).append(")");
    sb.append("(").append(_metric).append(")");
    sb.append(_comparator.toString());
    sb.append("(").append(_value).append(")");
    return sb.toString();
  }

  public static AlertName from(String alertNameStr) {
    if (alertNameStr == null) {
      throw new NullPointerException("alertNameStr is null");
    }

    // split (alertName)(metric)cmp(value) to [ , alertName, , metric, cmp, value]
    String[] parts = alertNameStr.split("[()]");
    if (parts == null || parts.length != 6 || !parts[0].isEmpty() || !parts[2].isEmpty()) {
      throw new IllegalArgumentException(
          "AlertName is NOT in form of (scope)(metric)comparator(value), was " + alertNameStr);
    }

    String[] scopeParts = parts[1].split("\\.");

    String metric = parts[3];
    AlertComparator cmp = AlertComparator.from(parts[4]);
    if (cmp == null) {
      throw new IllegalArgumentException("Invalid alert comparator, was " + parts[4]);
    }
    String value = parts[5];

    AlertScope scope = new AlertScope(scopeParts);
    return new AlertName(scope, metric, cmp, value);
  }

  public boolean match(AlertName alertName) {
    return _scope.match(alertName._scope) && _metric.equals(alertName._metric)
        && _comparator == alertName._comparator && _value.equals(alertName._value);
  }

  public AlertScope getScope() {
    return _scope;
  }
}
