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
package org.apache.sqoop.integration.server;

import org.apache.sqoop.client.ClientError;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.error.code.DriverError;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Ensure that server will reject starting job when either job itself
 * or corresponding link is disabled.
 */
@Infrastructure(dependencies = {HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class})
public class SubmissionWithDisabledModelObjectsTest extends SqoopTestCase {

  private boolean enabledLink;
  private boolean enabledJob;

  @Factory(dataProvider="submission-with-disable-model-objects-integration-test")
  public SubmissionWithDisabledModelObjectsTest(boolean enabledLink, boolean enabledJob) {
    this.enabledLink = enabledLink;
    this.enabledJob = enabledJob;
  }

  @DataProvider(name="submission-with-disable-model-objects-integration-test", parallel=true)
  public static Object[][] data() {
    return new Object[][]{
        {true, false},
        {false, true},
        {false, false},
    };
  }

  @BeforeMethod
  public void setupRdbmsTable() {
    createAndLoadTableCities();
  }

  @AfterMethod
  public void tearDownRdbmsTable() {
    dropTable();
    clearJob();
    clearLink();
  }

  @Test
  public void testWithDisabledObjects() throws Exception {
    // the test will be executed several times, and the jobName should be different.
    String jobName = "job_" + System.currentTimeMillis();
    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLinkConfig(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getName(), hdfsLink.getName());
    job.setName(jobName);

    // rdms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    saveJob(job);

    // Disable model entities as per parameterized run
    getClient().enableLink(rdbmsLink.getName(), enabledLink);
    getClient().enableJob(jobName, enabledJob);

    // Try to execute the job and verify that the it was not executed
    try {
      executeJob(jobName);
      fail("Expected exception as the model classes are disabled.");
    } catch(SqoopException ex) {
      // Top level exception should be CLIENT_0001
      assertEquals(ClientError.CLIENT_0001, ex.getErrorCode());

      // We can directly verify the ErrorCode from SqoopException as client side
      // is not rebuilding SqoopExceptions per missing ErrorCodes. E.g. the cause
      // will be generic Throwable and not SqoopException instance.
      Throwable cause = ex.getCause();
      assertNotNull(cause);

      if(!enabledJob) {
        assertTrue(cause.getMessage().startsWith(DriverError.DRIVER_0009.toString()));
      } else if(!enabledLink) {
        assertTrue(cause.getMessage().startsWith(DriverError.DRIVER_0010.toString()));
      } else {
        fail("Unexpected expception retrieved from server " + cause);
      }
    }
  }

  // The default table name is too long for oracle
  @Override
  public TableName getTableName() {
    return new TableName("DisabledObjectsTest");
  }
}
