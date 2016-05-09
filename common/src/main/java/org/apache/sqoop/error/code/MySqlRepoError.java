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
package org.apache.sqoop.error.code;

import org.apache.sqoop.common.ErrorCode;

public enum MySqlRepoError implements ErrorCode {

  /** An unknown error has occurred. */
  MYSQLREPO_0000("An unknown error has occurred"),

  /** The MySQL Repository handler was unable to add directions. */
  MYSQLREPO_0001("Could not add directions"),

  /** The system was unable to get ID of recently added direction. */
  MYSQLREPO_0002("Could not get ID of recently added direction"),

  ;

  private final String message;

  private MySqlRepoError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }

}
