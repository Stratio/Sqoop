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
package org.apache.sqoop.connector.jdbc.configuration;

import java.io.Serializable;

import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;

/**
 * Enables user to configure various aspects of the way the JDBC Connector generates
 * SQL queries.
 */
@ConfigClass
public class SqlDialect implements Serializable{
  /**
   * Character(s) that we should use to escape SQL identifiers (tables, column names, ...)
   */
  @Input(size = 5)  public String identifierEnclose;
}
