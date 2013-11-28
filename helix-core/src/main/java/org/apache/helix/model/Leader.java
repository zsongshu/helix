package org.apache.helix.model;

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

import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.ControllerId;

/**
 * A special live instance that represents the leader controller
 */
public class Leader extends LiveInstance {
  public enum LeaderProperty {
    ZKPROPERTYTRANSFERURL,
    MONITORING_HOST,
    MONITORING_PORT
  }

  /**
   * Instantiate with a controller identifier
   * @param controllerId typed controller identifier
   */
  public Leader(ControllerId controllerId) {
    super(controllerId.toString());
  }

  /**
   * Instantiate with a pre-populated record
   * @param record ZNRecord corresponding to a live instance
   */
  public Leader(ZNRecord record) {
    super(record);
  }

  /**
   * Get a web service URL where ZK properties can be transferred to
   * @return a fully-qualified URL
   */
  public String getWebserviceUrl() {
    return _record.getSimpleField(LeaderProperty.ZKPROPERTYTRANSFERURL.toString());
  }

  /**
   * Set a web service URL where ZK properties can be transferred to
   * @param url a fully-qualified URL
   */
  public void setWebserviceUrl(String url) {
    _record.setSimpleField(LeaderProperty.ZKPROPERTYTRANSFERURL.toString(), url);
  }

  /**
   * Get the monitoring port attached to this instance
   * @return port, or -1
   */
  public int getMonitoringPort() {
    return _record.getIntField(LeaderProperty.MONITORING_PORT.toString(), -1);
  }

  /**
   * Set the monitoring port attached to this instance
   * @param port the port to contact for monitoring
   */
  public void setMonitoringPort(int port) {
    _record.setIntField(LeaderProperty.MONITORING_PORT.toString(), port);
  }

  /**
   * Get the monitoring host attached to this instance
   * @return host, or null
   */
  public String getMonitoringHost() {
    return _record.getSimpleField(LeaderProperty.MONITORING_HOST.toString());
  }

  /**
   * Set the monitoring host attached to this instance
   * @param host the host to contact for monitoring
   */
  public void setMonitoringHost(String host) {
    _record.setSimpleField(LeaderProperty.MONITORING_HOST.toString(), host);
  }
}
