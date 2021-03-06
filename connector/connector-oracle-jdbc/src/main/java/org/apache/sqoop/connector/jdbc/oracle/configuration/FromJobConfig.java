/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.connector.jdbc.oracle.configuration;

import java.io.Serializable;
import java.util.List;

import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.validators.NotEmpty;

/**
 *
 */
@ConfigClass
public class FromJobConfig implements Serializable {

  @Input(size = 2000, validators = { @Validator(NotEmpty.class)})
  public String tableName;

  @Input
  public List<String> columns;

  @Input
  public Boolean consistentRead;

  @Input
  public Long consistentReadScn;

  @Input
  public List<String> partitionList;

  @Input
  public OracleUtilities.OracleDataChunkMethod dataChunkMethod;

  @Input
  public OracleUtilities.OracleBlockToSplitAllocationMethod
      dataChunkAllocationMethod;

  @Input
  public OracleUtilities.OracleTableImportWhereClauseLocation
      whereClauseLocation;

  @Input
  public Boolean omitLobColumns;

  @Input(size = 2000)
  public String queryHint;

  @Input(size = 2000)
  public String conditions;

}
