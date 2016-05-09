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
package org.apache.sqoop.job.etl;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.ImmutableContext;

/**
 * Base context class for the {@link Transferable} components
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class TransferableContext {

  ImmutableContext context;

  private String user;

  public TransferableContext(ImmutableContext context, String user) {
    this.context = context;
    this.user = user;
  }

  /**
   * Context object associated with the particular actor
   *
   * @return
   */
  public ImmutableContext getContext() {
    return context;
  }

  /**
   * Convenience method that will return value from wrapped context class.
   */
  public String getString(String key) {
    return context.getString(key);
  }

  /**
   * Convenience method that will return value from wrapped context class.
   */
  public String getString(String key, String defaultValue) {
    return context.getString(key, defaultValue);
  }

  /**
   * Convenience method that will return value from wrapped context class.
   */
  public long getLong(String key, long defaultValue) {
    return context.getLong(key, defaultValue);
  }

  /**
   * Convenience method that will return value from wrapped context class.
   */
  public int getInt(String key, int defaultValue) {
    return context.getInt(key, defaultValue);
  }

  /**
   * Convenience method that will return value from wrapped context class.
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    return context.getBoolean(key, defaultValue);
  }

  /**
   * Return user who started this job (e.g. the one who run "start job" in shell)
   */
  public String getUser() {
    return user;
  }
}
