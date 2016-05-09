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
package org.apache.sqoop.connector.jdbc.oracle.util;

import java.io.Serializable;

/**
 * Contains details about a partition for an Oracle table.
 */
public class OracleTablePartition implements Serializable {

  private String name;
  private boolean isSubPartition;

  public OracleTablePartition(String name, boolean isSubPartition) {
    this.setName(name);
    this.setSubPartition(isSubPartition);
  }

  public String getName() {
    return name;
  }

  public void setName(String newName) {
    this.name = newName;
  }

  public boolean isSubPartition() {
    return isSubPartition;
  }

  public void setSubPartition(boolean newIsSubPartition) {
    this.isSubPartition = newIsSubPartition;
  }

}
