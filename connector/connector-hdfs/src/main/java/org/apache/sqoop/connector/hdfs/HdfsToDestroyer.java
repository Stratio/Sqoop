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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.hdfs.security.SecurityUtils;
import org.apache.sqoop.error.code.HdfsConnectorError;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;

import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;

public class HdfsToDestroyer extends Destroyer<LinkConfiguration, ToJobConfiguration> implements Serializable {

  private static final Logger LOG = Logger.getLogger(HdfsToDestroyer.class);

  /**
   * {@inheritDoc}
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"})
  public void destroy(final DestroyerContext context, final LinkConfiguration linkConfig, final ToJobConfiguration jobConfig) {
    final Configuration configuration = new Configuration();
    HdfsUtils.contextToConfiguration(context.getContext(), configuration);

    final String workingDirectory = context.getString(HdfsConstants.WORK_DIRECTORY);
    final Path targetDirectory = new Path(jobConfig.toJobConfig.outputDirectory);

    try {
      SecurityUtils.createProxyUserAndLoadDelegationTokens(context).doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          FileSystem fs = FileSystem.get(configuration);

          // If we succeeded, we need to move all files from working directory
          if (context.isSuccess()) {
            FileStatus[] fileStatuses = fs.listStatus(new Path
              (workingDirectory));
            for (FileStatus status : fileStatuses) {
              LOG.info("Committing file: " + status.getPath().toString() + " " +
                "of size " + status.getLen());
              fs.rename(status.getPath(), new Path(targetDirectory, status
                .getPath().getName()));
            }
          }

          // Clean up working directory
          fs.delete(new Path(workingDirectory), true);
          return null;
        }
      });
    } catch (Exception e) {
      throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0008, e);
    }
  }
}
