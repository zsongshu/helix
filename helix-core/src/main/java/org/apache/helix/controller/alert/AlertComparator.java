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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum AlertComparator {
  LARGER(">"),
  SMALLER("<"),
  NOT_EQUAL("!=");

  private static final Map<String, AlertComparator> _indexMap;
  private final String _op;

  static {
    Map<String, AlertComparator> aMap = new HashMap<String, AlertComparator>();
    for (AlertComparator cmp : AlertComparator.values()) {
      aMap.put(cmp._op, cmp);
    }
    _indexMap = Collections.unmodifiableMap(aMap);
  }

  private AlertComparator(String op) {
    _op = op;
  }

  @Override
  public String toString() {
    return _op;
  }

  public static AlertComparator from(String op) {
    return _indexMap.get(op);
  }
}
