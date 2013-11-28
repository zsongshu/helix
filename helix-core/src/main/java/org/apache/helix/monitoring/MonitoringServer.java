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

import org.apache.helix.HelixDataAccessor;

/**
 * Generic interface for a monitoring service that should be attached to a controller.
 */
public interface MonitoringServer {
  /**
   * Start the monitoring service synchronously
   */
  public void start();

  /**
   * Stop the monitoring service synchronously
   */
  public void stop();

  /**
   * Add a collection of configuration files
   * @param accessor HelixDataAccessor that can reach Helix's backing store
   */
  public void addConfigs(HelixDataAccessor accessor);

  /**
   * Check if the service has been started
   * @return true if started, false otherwise
   */
  public boolean isStarted();

  /**
   * Get the host of the service
   * @return String hostname
   */
  public String getHost();

  /**
   * Get the port of the service
   * @return integer port
   */
  public int getPort();
}
