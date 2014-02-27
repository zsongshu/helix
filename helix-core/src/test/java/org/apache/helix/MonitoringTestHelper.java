package org.apache.helix;

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

import java.io.IOException;
import java.net.ServerSocket;

import org.I0Itec.zkclient.NetworkUtil;

public class MonitoringTestHelper {
  static final int MAX_PORT = 65535;

  /**
   * generate a default riemann.config
   * @param riemannPort
   * @return
   */
  public static String getRiemannConfigString(int riemannPort) {
    StringBuilder sb = new StringBuilder();
    sb.append("(logging/init :file \"/dev/null\")\n\n")
        .append("(tcp-server :host \"0.0.0.0\" :port " + riemannPort + ")\n\n")
        .append("(instrumentation {:interval 1})\n\n")
        .append("; (udp-server :host \"0.0.0.0\")\n")
        .append("; (ws-server :host \"0.0.0.0\")\n")
        .append("; (repl-server :host \"0.0.0.0\")\n\n")
        .append("(periodically-expire 1)\n\n")
        .append(
            "(let [index (default :ttl 3 (update-index (index)))]\n  (streams\n    (expired prn)\n    index))\n");

    return sb.toString();
  }

  /**
   * generate a test config for checking latency
   * @param proxyPort
   * @return
   */
  public static String getLatencyCheckConfigString(int proxyPort)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("(require 'riemann.config)\n")
      .append("(require 'clj-http.client)\n\n")
      .append("(defn parse-double\n  \"Convert a string into a double\"\n  ")
      .append("[instr]\n  (Double/parseDouble instr))\n\n")
      .append("(defn check-95th-latency\n  \"Check if the 95th percentile latency is within expectations\"\n  ")
      .append("[e]\n  (let [latency (parse-double (:latency95 e))]\n    ")
      .append("(if (> latency 1.0) \n      ; Report if the 95th percentile latency exceeds 1.0s\n      ")
      .append("(do (prn (:host e) \"has an unacceptable 95th percentile latency of\" latency)\n      ")
      .append("(let [alert-name-str (str \"(\" (:cluster e) \".%.\" (:host e) \")(latency95)>(1000)\" )\n        ")
      .append("proxy-url (str \"http://localhost:\" " + proxyPort + " )]\n        ")
      .append("(clj-http.client/post proxy-url {:body alert-name-str }))))))\n\n")
      .append("(streams\n  (where\n    ; Only process services containing LatencyReport\n    ")
      .append("(and (service #\".*LatencyReport.*\") (not (state \"expired\")))\n    ")
      .append("check-95th-latency))\n");
    
    return sb.toString();
  }
  
  /**
   * find an available tcp port
   * @return
   */
  public static int availableTcpPort() {
    ServerSocket ss = null;
    try {
      ss = new ServerSocket(0);
      ss.setReuseAddress(true);
      return ss.getLocalPort();
    } catch (IOException e) {
      // ok
    } finally {
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          // should not be thrown
        }
      }
    }
    return -1;
  }

  /**
   * find the first available port starting from startPort inclusive
   * @param startPort
   * @return
   */
  public static int availableTcpPort(int startPort) {
    int port = startPort;
    for (; port <= MAX_PORT; port++) {
      if (NetworkUtil.isPortFree(port))
        break;
    }

    return port > MAX_PORT ? -1 : port;
  }
}
