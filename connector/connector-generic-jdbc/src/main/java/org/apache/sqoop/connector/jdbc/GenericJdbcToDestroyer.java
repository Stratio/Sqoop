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
package org.apache.sqoop.connector.jdbc;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;

public class GenericJdbcToDestroyer extends Destroyer<LinkConfiguration, ToJobConfiguration> implements Serializable {

  private static final Logger LOG = Logger.getLogger(GenericJdbcToDestroyer.class);

  @Override
  public void destroy(DestroyerContext context, LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) {
    LOG.info("Running generic JDBC connector destroyer");

    final String tableName = toJobConfig.toJobConfig.tableName;
    final String stageTableName = toJobConfig.toJobConfig.stageTableName;
    final boolean stageEnabled = stageTableName != null &&
      stageTableName.length() > 0;
    if(stageEnabled) {
      moveDataToDestinationTable(linkConfig,
        context.isSuccess(), stageTableName, tableName);
    }
  }

  private void moveDataToDestinationTable(LinkConfiguration linkConfig, boolean success, String stageTableName, String tableName) {
    GenericJdbcExecutor executor = new GenericJdbcExecutor(linkConfig);
    try {
      if(success) {
        LOG.info("Job completed, transferring data from stage fromTable to " +
          "destination fromTable.");
        executor.migrateData(stageTableName, tableName);
      } else {
        LOG.warn("Job failed, clearing stage fromTable.");
        executor.deleteTableData(stageTableName);
      }
    } finally {
	  executor.close();
    }
  }

}
