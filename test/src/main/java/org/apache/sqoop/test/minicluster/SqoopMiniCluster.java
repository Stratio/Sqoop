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
package org.apache.sqoop.test.minicluster;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.common.test.repository.RepositoryProviderFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Basic tools to bootstrap Sqoop Mini cluster.
 */
public abstract class SqoopMiniCluster {

  /**
   * Hadoop configuration.
   *
   * Either mini cluster generated or real one if we're running on real cluster.
   */
  private Configuration configuration;

  /**
   * Temporary path for storing Sqoop server data (configuration files)
   */
  private String temporaryPath;

  /**
   * Create Sqoop Mini cluster with default configuration
   *
   * @param temporaryPath Temporary path
   * @throws Exception
   */
  public SqoopMiniCluster(String temporaryPath) throws Exception {
    this(temporaryPath, new Configuration());
  }

  /**
   * Create Sqoop Mini cluster
   *
   * @param temporaryPath Temporary path
   * @param configuration Hadoop configuration
   * @throws Exception
   */
  public SqoopMiniCluster(String temporaryPath, Configuration configuration) throws Exception {
    this.temporaryPath = temporaryPath;
    this.configuration = configuration;
  }

  /**
   * Return temporary path
   *
   * @return Temporary path
   */
  public String getTemporaryPath() {
    return temporaryPath;
  }

  public String getConfigurationPath() {
    return temporaryPath + "/config/";
  }

  public String getLogPath() {
    return temporaryPath + "/log/";
  }

  /**
   * Start Sqoop Mini cluster
   *
   * @throws Exception
   */
  abstract public void start() throws Exception;

  /**
   * Stop Sqoop mini cluster
   *
   * @throws Exception
   */
  abstract public void stop() throws Exception;

  /**
   * @return server URL (e.g. http://localhost:12000/sqoop)
   */
  abstract public String getServerUrl();

  /**
   * Prepare temporary directory for starting Sqoop server.
   *
   * @throws IOException
   */
  protected void prepareTemporaryPath() throws Exception {
    File tmpDir = new File(getTemporaryPath());
    File configDir = new File(getConfigurationPath());
    File logDir = new File(getLogPath());

    FileUtils.deleteDirectory(tmpDir);
    FileUtils.forceMkdir(tmpDir);
    FileUtils.forceMkdir(configDir);
    FileUtils.forceMkdir(logDir);

    // Create configuration files
    System.setProperty(ConfigurationConstants.SYSPROP_CONFIG_DIR, getConfigurationPath());

    // sqoop_bootstrap.properties
    FileUtils.writeStringToFile(new File(getConfigurationPath() + "sqoop_bootstrap.properties"), "sqoop.config.provider=org.apache.sqoop.core.PropertiesConfigurationProvider");

    // sqoop.properties
    // TODO: This should be generated more dynamically so that user can specify Repository, Submission and Execution engines
    File f = new File(getConfigurationPath() + "sqoop.properties");

    List<String> sqoopProperties = new LinkedList<String>();
    mapToProperties(sqoopProperties, getLoggerConfiguration());
    mapToProperties(sqoopProperties, getRepositoryConfiguration());
    mapToProperties(sqoopProperties, getSubmissionEngineConfiguration());
    mapToProperties(sqoopProperties, getExecutionEngineConfiguration());
    mapToProperties(sqoopProperties, getSecurityConfiguration());
    mapToProperties(sqoopProperties, getConnectorManagerConfiguration());
    mapToProperties(sqoopProperties, getDriverManagerConfiguration());
    mapToProperties(sqoopProperties, getClasspathConfiguration());
    mapToProperties(sqoopProperties, getBlacklistedConnectorConfiguration());

    FileUtils.writeLines(f, sqoopProperties);

    // Hadoop configuration
    OutputStream stream = FileUtils.openOutputStream(new File(getConfigurationPath() + "hadoop-site.xml"));
    configuration.writeXml(stream);
    stream.close();
  }

  private void mapToProperties(List<String> output, Map<String, String> input) {
    for(Map.Entry<String, String> entry : input.entrySet()) {
      output.add(entry.getKey() + "=" + entry.getValue());
    }
  }
  /**
   * Return properties for logger configuration.
   *
   * Default implementation will configure server to log into console.
   *
   * @return
   */
  protected Map<String, String> getLoggerConfiguration() {
    Map<String, String> properties = new HashMap<String, String>();

    properties.put("org.apache.sqoop.log4j.appender.file", "org.apache.log4j.ConsoleAppender");
    properties.put("org.apache.sqoop.log4j.appender.file.layout", "org.apache.log4j.PatternLayout");
    properties.put("org.apache.sqoop.log4j.appender.file.layout.ConversionPattern", "%d{ISO8601} %-5p [%l] %m%n");
    properties.put("org.apache.sqoop.log4j.debug", "true");
    properties.put("org.apache.sqoop.log4j.rootCategory", "DEBUG, file");

    return properties;
  }

  protected Map<String, String> getRepositoryConfiguration() throws Exception {
    return RepositoryProviderFactory.getRepositoryProperties();
  }

  protected Map<String, String> getSubmissionEngineConfiguration() {
    Map<String, String> properties = new HashMap<String, String>();

    properties.put("org.apache.sqoop.submission.engine", "org.apache.sqoop.submission.mapreduce.MapreduceSubmissionEngine");
    properties.put("org.apache.sqoop.submission.engine.mapreduce.configuration.directory", getConfigurationPath());

    return properties;
  }

  protected Map<String, String> getExecutionEngineConfiguration() {
    Map<String, String> properties = new HashMap<String, String>();

    properties.put("org.apache.sqoop.execution.engine", "org.apache.sqoop.execution.mapreduce.MapreduceExecutionEngine");

    return properties;
  }

  protected Map<String, String> getSecurityConfiguration() {
    Map<String, String> properties = new HashMap<String, String>();

    properties.put("org.apache.sqoop.authentication.type", "SIMPLE");
    properties.put("org.apache.sqoop.authentication.handler", "org.apache.sqoop.security.SimpleAuthenticationHandler");

    /**
     * Due to the fact that we share a JVM with hadoop during unit testing,
     * proxy user configuration is also shared with hadoop.
     *
     * We need to enable impersonation on hadoop for our map reduce jobs
     * (normally this would be accomplished with "hadoop.proxyuser"), so we
     * pass it through sqoop configuration
     */
    String user = System.getProperty("user.name");
    properties.put("org.apache.sqoop.authentication.proxyuser." + user + ".groups", "*");
    properties.put("org.apache.sqoop.authentication.proxyuser." + user + ".hosts", "*");

    return properties;
  }

  protected Map<String, String> getConnectorManagerConfiguration() {
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(ConfigurationConstants.CONNECTOR_AUTO_UPGRADE, "true");
    return properties;
  }

  protected Map<String, String> getDriverManagerConfiguration() {
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(ConfigurationConstants.DRIVER_AUTO_UPGRADE, "true");
    return properties;
  }

  protected Map<String, String> getClasspathConfiguration() {
    return MapUtils.EMPTY_MAP;
  }

  protected Map<String, String> getBlacklistedConnectorConfiguration() {
    return MapUtils.EMPTY_MAP;
  }
}
