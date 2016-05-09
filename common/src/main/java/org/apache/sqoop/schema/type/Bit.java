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
package org.apache.sqoop.schema.type;

import java.io.Serializable;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * True/False value.
 *
 * JDBC Types: bit, boolean
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Bit extends Column implements Serializable {

  public Bit(String name) {
    super(name);
  }

  public Bit(String name, Boolean nullable) {
    super(name, nullable);
  }

  @Override
  public ColumnType getType() {
    return ColumnType.BIT;
  }

  @Override
  public String toString() {
    return new StringBuilder("Bit{")
      .append(super.toString())
      .append("}")
      .toString();
  }

}
