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

import java.util.concurrent.TimeUnit;

import org.apache.helix.api.id.ResourceId;

/**
 * Interface for a client that can register with a monitoring server and send events for monitoring
 */
public interface MonitoringClient {
  /**
   * Connect. May be asynchronous.
   * @throws Exception
   */
  void connect() throws Exception;

  /**
   * Disconnect synchronously.
   */
  void disconnect();

  /**
   * Send an event
   * @param resource
   * @param e the event
   * @param batch true if this should be part of a batch operation
   * @return true if the event was sent (or queued for batching), false otherwise
   */
  boolean send(ResourceId resource, MonitoringEvent e, boolean batch);

  /**
   * Send an event and flush any outstanding messages
   * @param resource
   * @param e the event
   * @return true if events were successfully sent, false otherwise
   */
  boolean sendAndFlush(ResourceId resource, MonitoringEvent e);

  /**
   * Schedule an operation to run
   * @param resource
   * @param interval the frequency
   * @param delay the amount of time to wait before the first execution
   * @param unit the unit of time to use
   * @param r the code to run
   */
  void every(ResourceId resource, long interval, long delay, TimeUnit unit, Runnable r);

  /**
   * Check if there is a valid connection to a monitoring server
   * @return true if connected, false otherwise
   */
  boolean isConnected();

  /**
   * Check if batching is being used
   * @return true if enabled, false otherwise
   */
  boolean isBatchingEnabled();

  /**
   * Check the number of events sent as a batch
   * @return the batch size, or 1 if batching is not used
   */
  int getBatchSize();

  /**
   * Flush all outstanding events
   * @return true if all events were flushed, false otherwise
   */
  boolean flush();
}
