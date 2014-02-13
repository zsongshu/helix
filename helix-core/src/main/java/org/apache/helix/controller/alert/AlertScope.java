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

import java.util.Arrays;

import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;

public class AlertScope {
  public static final String WILDCARD = "%";

  /**
   * DON'T change the sequence
   */
  public enum AlertScopeField {
    cluster,
    tenant,
    node,
    resource,
    partition;
  }

  private final String[] _scopeFields;

  public AlertScope(Id clusterId, Id tenantId, Id nodeId, Id resourceId, Id partitionId) {
    this(clusterId == null ? null : clusterId.stringify(), tenantId == null ? null : tenantId
        .stringify(), nodeId == null ? null : nodeId.stringify(), resourceId == null ? null
        : resourceId.stringify(), partitionId == null ? null : partitionId.stringify());
  }

  public AlertScope(String... fields) {
    int maxArgNum = AlertScopeField.values().length;
    if (fields != null && fields.length > maxArgNum) {
      throw new IllegalArgumentException("Too many arguments. Should be no more than " + maxArgNum
           + " but was " + fields.length + ", fields: " + Arrays.asList(fields));
    }
    // all array elements are init'ed to null
    _scopeFields = new String[AlertScopeField.values().length];

    if (fields != null) {
      for (int i = 0; i < fields.length; i++) {
        if (!WILDCARD.equals(fields[i])) {
          _scopeFields[i] = fields[i];
        }
      }
    }
  }

  /**
   * represent AlertScope in form of:
   * {cluster}.{tenant}.{node}.{resource}.{partition} using % for null field
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String scopeField : _scopeFields) {
      if (sb.length() > 0) {
        sb.append(".");
      }
      sb.append(scopeField == null ? WILDCARD : scopeField);
    }

    return sb.toString();
  }

  public String get(AlertScopeField field) {
    return _scopeFields[field.ordinal()];
  }

  public ClusterId getClusterId() {
    return ClusterId.from(get(AlertScopeField.cluster));
  }

  /**
   * match two alert-scopes, null means "don't care"
   * @param scope
   * @return
   */
  public boolean match(AlertScope scope) {
    for (int i = 0; i < _scopeFields.length; i++) {
      if (_scopeFields[i] != null && scope._scopeFields[i] != null
          && !_scopeFields[i].equals(scope._scopeFields[i])) {
        return false;
      }
    }
    return true;
  }
}