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
package org.apache.sqoop.connector.hdfs;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;
import org.joda.time.DateTime;

public class HdfsFromDestroyer extends Destroyer<LinkConfiguration, FromJobConfiguration> implements Serializable {

  private static final Logger LOG = Logger.getLogger(HdfsFromDestroyer.class);

  /**
   * Callback to clean up after job execution.
   *
   * @param context Destroyer context
   * @param linkConfig link configuration object
   * @param jobConfig FROM job configuration object
   */
  @Override
  public void destroy(DestroyerContext context, LinkConfiguration linkConfig, FromJobConfiguration jobConfig) {
    LOG.info("Running HDFS connector destroyer");
  }

  @Override
  public void updateConfiguration(DestroyerContext context, LinkConfiguration linkConfiguration, FromJobConfiguration jobConfiguration) {
    LOG.info("Updating HDFS connector options");
    long epoch = context.getLong(HdfsConstants.MAX_IMPORT_DATE, -1);
    jobConfiguration.incremental.lastImportedDate = epoch == -1 ? null : new DateTime(epoch);
  }
}
