package org.apache.helix.manager.zk;

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

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.controller.alert.AlertAction;
import org.apache.helix.model.AlertConfig;
import org.apache.helix.controller.alert.AlertName;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.tools.ClusterSetup;
import org.apache.log4j.Logger;

public class DefaultAlertMsgHandlerFactory implements MessageHandlerFactory {
  private static final Logger LOG = Logger.getLogger(DefaultAlertMsgHandlerFactory.class);
  public static final String DEFAULT_ALERT_CONFIG = "default";

  public static class DefaultAlertMsgHandler extends MessageHandler {
    public DefaultAlertMsgHandler(Message message, NotificationContext context) {
      super(message, context);
    }

    @Override
    public HelixTaskResult handleMessage() throws InterruptedException {
      HelixManager manager = _notificationContext.getManager();
      HelixTaskResult result = new HelixTaskResult();

      // get alert-name from message
      String alertNameStr = _message.getAttribute(Attributes.ALERT_NAME);
      AlertName alertName = AlertName.from(alertNameStr);

      // get action from alert config
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      AlertConfig defaultAlertConfig =
          accessor.getProperty(accessor.keyBuilder().alertConfig(DEFAULT_ALERT_CONFIG));
      AlertAction action = defaultAlertConfig.findAlertAction(alertName);

      if (action != null) {
        // perform action
        HelixAdmin admin = manager.getClusterManagmentTool();
        try {
          ClusterSetup setupTool = new ClusterSetup(admin);
          ClusterSetup.processCommandLineArgs(setupTool, action.getCli());
          result.setSuccess(true);

        } catch (Exception e) {
          String errMsg = "Exception execute action: " + action + " for alert: " + alertNameStr;
          result.setSuccess(false);
          result.setMessage(errMsg);
          result.setException(e);
          LOG.error(errMsg, e);
        }
      } else {
        String errMsg = "Could NOT find action for alert: " + alertNameStr;
        result.setSuccess(false);
        result.setMessage(errMsg);
        LOG.error(errMsg);
      }

      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOG.error("Error processing message: " + _message + ", errCode: " + code + ", errType: "
          + type, e);
    }

  }

  public DefaultAlertMsgHandlerFactory() {
    LOG.info("construct default alert message handler factory");
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    String type = message.getMsgType();

    if (!type.equals(getMessageType())) {
      throw new HelixException("Unexpected msg type for message " + message.getMessageId()
           + " type:" + message.getMsgType());
    }

    return new DefaultAlertMsgHandler(message, context);

  }

  @Override
  public String getMessageType() {
    return Message.MessageType.ALERT.name();
  }

  @Override
  public void reset() {
    LOG.info("reset default alert message handler factory");
  }

}