/*
 * Copyright (C) 2016 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * Mutable variant of context class for "special" usage
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MutableMapContext extends MapContext implements MutableContext, Serializable {

  public MutableMapContext(Map<String, String> options) {
    super(options);
  }

  public MutableMapContext() {
    this(new HashMap<String, String>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setString(String key, String value) {
    getOptions().put(key, value);
  }

  @Override
  public void setLong(String key, long value) {
    getOptions().put(key, Long.toString(value));
  }

  @Override
  public void setInteger(String key, int value) {
    getOptions().put(key, Integer.toString(value));
  }

  @Override
  public void setBoolean(String key, boolean value) {
    getOptions().put(key, Boolean.toString(value));
  }

  @Override
  public void setAll(Map<String, String> map) {
    if(map == null) {
      return;
    }

    getOptions().putAll(map);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return getOptions().entrySet().iterator();
  }
}
