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
package org.apache.sqoop.integration.repository.mysql;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.test.db.MySQLProvider;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.json.DriverBean;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.repository.common.CommonRepositorySchemaConstants;
import org.apache.sqoop.repository.mysql.MySqlRepositoryHandler;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.testng.annotations.*;
import org.mockito.Mockito;

/**
 * Abstract class with convenience methods for testing mysql repository.
 */
abstract public class MySqlTestCase {
  public static MySQLProvider provider;
  public static MySqlTestUtils utils;
  public MySqlRepositoryHandler handler;
  private ConnectorManager mockConnectorManager;

  @BeforeClass(alwaysRun = true)
  public void setUpClass() throws Exception {
    provider = new MySQLProvider();
    utils = new MySqlTestUtils(provider);

    mockConnectorManager = Mockito.mock(ConnectorManager.class);
    Mockito.when(mockConnectorManager.getConnectorConfigurable("A")).thenReturn(getConnector(true, true, "A", "org.apache.sqoop.test.A"));
    Mockito.when(mockConnectorManager.getConnectorConfigurable("B")).thenReturn(getConnector(true, true, "B", "org.apache.sqoop.test.B"));
    ConnectorManager.setInstance(mockConnectorManager);
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    provider.start();

    handler = new MySqlRepositoryHandler();
    handler.createOrUpgradeRepository(provider.getConnection());
    utils.setDatabase(CommonRepositorySchemaConstants.SCHEMA_SQOOP);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {
    provider.dropDatabase(CommonRepositorySchemaConstants.SCHEMA_SQOOP);
    provider.stop();
  }

  protected MConnector getConnector(String name, String className,
      String version, boolean from, boolean to) {
    return new MConnector(name, className, version, getLinkConfig(),
        from ? getFromConfig() : null, to ? getToConfig() : null);
  }

  protected MDriver getDriver() {
    return new MDriver(getDriverConfig(), DriverBean.CURRENT_DRIVER_VERSION);
  }

  protected MLink getLink(String name, MConnector connector) {
    MLink link = new MLink(connector.getUniqueName(),
        connector.getLinkConfig());
    link.setName(name);
    fillLink(link);
    return link;
  }

  protected MJob getJob(String name, MConnector connectorA,
      MConnector connectorB, MLink linkA, MLink linkB) {
    MDriver driver = handler.findDriver(MDriver.DRIVER_NAME,
        provider.getConnection());
    MJob job = new MJob(connectorA.getUniqueName(),
        connectorB.getUniqueName(), linkA.getName(),
        linkB.getName(), connectorA.getFromConfig(),
        connectorB.getToConfig(), driver.getDriverConfig());
    job.setName(name);
    fillJob(job);

    return job;
  }

  protected MSubmission getSubmission(MJob job,
      SubmissionStatus submissionStatus) {
    MSubmission submission = new MSubmission(job.getPersistenceId(),
        new Date(), submissionStatus);
    fillSubmission(submission);
    return submission;
  }

  protected void fillLink(MLink link) {
    List<MConfig> configs = link.getConnectorLinkConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");
  }

  protected void fillJob(MJob job) {
    List<MConfig> configs = job.getFromJobConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");

    configs = job.getToJobConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");

    configs = job.getDriverConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");
  }

  protected void fillSubmission(MSubmission submission) {
    Counters counters = new Counters();
    counters.addCounterGroup(new CounterGroup("test-1"));
    counters.addCounterGroup(new CounterGroup("test-2"));
    submission.setCounters(counters);
  }

  protected MLinkConfig getLinkConfig() {
    return new MLinkConfig(getConfigs("l1", "l2"), Collections.EMPTY_LIST);
  }

  protected MFromConfig getFromConfig() {
    return new MFromConfig(getConfigs("from1", "from2"), Collections.EMPTY_LIST);
  }

  protected MToConfig getToConfig() {
    return new MToConfig(getConfigs("to1", "to2"), Collections.EMPTY_LIST);
  }

  protected MDriverConfig getDriverConfig() {
    return new MDriverConfig(getConfigs("d1", "d2"), Collections.EMPTY_LIST);
  }

  protected List<MConfig> getConfigs(String configName1, String configName2) {
    List<MConfig> configs = new LinkedList<MConfig>();

    List<MInput<?>> inputs = new LinkedList<MInput<?>>();
    MInput<?> input = new MStringInput("I1", false, InputEditable.ANY,
        StringUtils.EMPTY, (short) 30, Collections.EMPTY_LIST);
    inputs.add(input);
    input = new MMapInput("I2", false, InputEditable.ANY, "I1", StringUtils.EMPTY, Collections.EMPTY_LIST);
    inputs.add(input);
    configs.add(new MConfig(configName1, inputs, Collections.EMPTY_LIST));

    inputs = new LinkedList<MInput<?>>();
    input = new MStringInput("I3", false, InputEditable.ANY, "I4", (short) 30, Collections.EMPTY_LIST);
    inputs.add(input);
    input = new MMapInput("I4", false, InputEditable.ANY, "I3", StringUtils.EMPTY, Collections.EMPTY_LIST);
    inputs.add(input);
    configs.add(new MConfig(configName2, inputs, Collections.EMPTY_LIST));

    return configs;
  }

  protected MConnector getConnector(boolean from, boolean to, String connectorName, String connectorClass) {
    MFromConfig fromConfig = null;
    MToConfig toConfig = null;
    if (from) {
      fromConfig = getFromConfig();
    }
    if (to) {
      toConfig = getToConfig();
    }
    return new MConnector(connectorName, connectorClass, "1.0-test", getLinkConfig(), fromConfig,
            toConfig);
  }
}
