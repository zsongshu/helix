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

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.alert.AlertAction;
import org.apache.helix.controller.alert.AlertName;
import org.apache.helix.controller.alert.AlertScope;
import org.apache.log4j.Logger;

public class AlertConfig extends HelixProperty {
  private static final Logger LOG = Logger.getLogger(AlertConfig.class);

  public AlertConfig(ZNRecord record) {
    super(record);
  }

  public AlertConfig(String id) {
    super(id);
  }

  public void putConfig(AlertName alertName, AlertAction action) {
    _record.setSimpleField(alertName.toString(), action.toString());
  }

  /**
   * Given an alert name, find the matching action
   * @param matchingName
   * @return
   */
  public AlertAction findAlertAction(AlertName matchingName) {
    // search each alert config entry to find a match
    for (String alertNameStr : _record.getSimpleFields().keySet()) {
      String actionStr = _record.getSimpleField(alertNameStr);

      try {
        AlertName name = AlertName.from(alertNameStr);
        if (!name.match(matchingName)) {
          continue;
        }

        // find a match
        // replace "{scope}" in action.args with actual values
        // e.g. "{node}" -> "localhost_12918"
        int start = actionStr.indexOf("{");
        while (start != -1) {
          int end = actionStr.indexOf("}", start + 1);
          String fieldStr = actionStr.substring(start + 1, end);
          AlertScope.AlertScopeField field = AlertScope.AlertScopeField.valueOf(fieldStr);

          String fieldValue = matchingName.getScope().get(field);
          if (fieldValue == null) {
            throw new NullPointerException("Null value for alert scope field: " + field
                + ", in alert: " + matchingName);
          }

          actionStr = actionStr.replace("{" + fieldStr + "}", fieldValue);
          start = actionStr.indexOf("{");
        }

        return AlertAction.from(actionStr);
      } catch (Exception e) {
        LOG.error("Exception in find action for alert: " + matchingName
            + ", matching alertEntry: \"" + alertNameStr + ":" + actionStr + "\"", e);
      }
    }

    return null;
  }

}
