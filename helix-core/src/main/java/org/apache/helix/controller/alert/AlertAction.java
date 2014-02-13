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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.helix.tools.ClusterSetup;

public class AlertAction {
  private final String[] _cliArgs;
  private final CommandLine _cli;

  public static class Builder {
    private String _cmd = null;
    private String[] _args = null;

    public Builder cmd(String cmd) {
      _cmd = cmd;
      return this;
    }

    public Builder args(String... args) {
      _args = args;
      return this;
    }

    public AlertAction build() {
      return new AlertAction(_cmd, _args);
    }
  }

  public AlertAction(String cmd, String[] args) {
    if (cmd == null) {
      throw new NullPointerException("command is null");
    }
    if (args == null) {
      args = new String[0];
    }

    // if arg is in form of "{..}", make sure it's a valid
    // alertScope
    for (String arg : args) {
      if (arg.startsWith("{") && arg.endsWith("}")) {
        try {
          String filedStr = arg.substring(1, arg.length() - 1);
          AlertScope.AlertScopeField.valueOf(filedStr);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Invalid alertScope value: " + arg + " in "
              + Arrays.asList(args));
        }
      }
    }

    _cliArgs = new String[args.length + 1];
    _cliArgs[0] = cmd;
    System.arraycopy(args, 0, _cliArgs, 1, args.length);
    _cli = parseCliArgs();

  }

  /**
   * represent alertAction in form of: (command)(arg1 arg2 ...)
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(").append(_cliArgs[0]).append(")");
    if (_cliArgs != null) {
      sb.append("(");
      for (int i = 1; i < _cliArgs.length; i++) {
        String arg = _cliArgs[i];
        if (i > 1) {
          sb.append(" ");
        }
        sb.append(arg);
      }
      sb.append(")");
    }
    return sb.toString();
  }

  public static AlertAction from(String actionStr) {
    if (actionStr == null) {
      throw new NullPointerException("actionStr is null");
    }

    // split (command)(arg1 arg2 ..) to [ , command, , arg1 arg2 ...]
    String[] parts = actionStr.split("[()]");

    if (parts.length == 2 && parts[0].isEmpty()) {
      return new AlertAction(parts[1], null);
    } else if (parts.length == 4 && parts[0].isEmpty() && parts[2].isEmpty()) {
      String[] args = parts[3].split("\\s+");
      return new AlertAction(parts[1], args);
    }
    throw new IllegalArgumentException("Invalid alerAction string, was " + actionStr);
  }

  public String[] getCliArgs() {
    return _cliArgs;
  }

  public CommandLine getCli() {
    return _cli;
  }

  private CommandLine parseCliArgs() {
    Options options = new Options();
    options.addOptionGroup(ClusterSetup.constructOptionGroup());

    try {
      String[] tmpCliArgs = Arrays.copyOf(_cliArgs, _cliArgs.length);

      // prepend "-" to helix-command (_cliArgs[0]), so it can be parsed
      tmpCliArgs[0] = String.format("-%s", tmpCliArgs[0]);
      return new GnuParser().parse(options, tmpCliArgs);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid HelixAdmin command: " + Arrays.toString(_cliArgs), e);
    }
  }
}
