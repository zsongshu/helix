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

import org.apache.helix.monitoring.MonitoringEvent;

import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.EventDSL;

public class ClientUtil {
  /**
   * Change a helix monitoring event into a Riemann event
   * @param c Riemann client
   * @param helixEvent helix event
   * @return Riemann EventDSL
   */
  public static EventDSL convertEvent(AbstractRiemannClient c, MonitoringEvent helixEvent) {
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
}
