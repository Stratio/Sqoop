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
package org.apache.sqoop.connector;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.core.Reconfigurable;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.core.SqoopConfiguration.CoreConfigurationListener;
import org.apache.sqoop.error.code.CommonRepositoryError;
import org.apache.sqoop.error.code.ConnectorError;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.repository.RepositoryTransaction;
import org.apache.sqoop.utils.ContextUtils;

public class ConnectorManager implements Reconfigurable {

  /**
   * Logger object.
   */
  private static final Logger LOG = Logger.getLogger(ConnectorManager.class);

  /**
   * Private instance to singleton of this class.
   */
  private static ConnectorManager instance;

  /**
   * Default connector auto upgrade option value
   */
  private static boolean DEFAULT_AUTO_UPGRADE = false;

  private Set<String> blacklistedConnectors;

  /**
   * Create default object by default.
   *
   * Every Sqoop server application needs one so this should not be performance issue.
   */
  static {
    instance = new ConnectorManager();
  }

  /**
   * The private constructor for the singleton class.
   */
  private ConnectorManager() {}

  /**
   * Return current instance.
   *
   * @return Current instance
   */
  public static ConnectorManager getInstance() {
    return instance;
  }

  /**
   * Allows to set instance in case that it's need.
   *
   * This method should not be normally used as the default instance should be sufficient. One target
   * user use case for this method are unit tests.
   *
   * @param newInstance New instance
   */
  public static void setInstance(ConnectorManager newInstance) {
    instance = newInstance;
  }

  // key: connector id, value: connector name
  private Map<Long, String> idToNameMap;
  private Set<String> connectorNames = new HashSet<String>();

  // key: connector name, value: connector handler
  private Map<String, ConnectorHandler> handlerMap;

  public List<MConnector> getConnectorConfigurables() {
    List<MConnector> connectors = new LinkedList<MConnector>();
    for (ConnectorHandler handler : handlerMap.values()) {
      connectors.add(handler.getConnectorConfigurable());
    }
    return connectors;
  }

  public Set<Long> getConnectorIds() {
    return idToNameMap.keySet();
  }

  public Map<String, ResourceBundle> getResourceBundles(Locale locale) {
    Map<String, ResourceBundle> bundles = new HashMap<String, ResourceBundle>();
    for (ConnectorHandler handler : handlerMap.values()) {
      String connectorName = handler.getConnectorConfigurable().getUniqueName();
      ResourceBundle bundle = handler.getSqoopConnector().getBundle(locale);
      bundles.put(connectorName, bundle);
    }
    return bundles;
  }

  public ResourceBundle getResourceBundle(long connectorId, Locale locale) {
    return getResourceBundle(idToNameMap.get(connectorId), locale);
  }

  public ResourceBundle getResourceBundle(String connectorName, Locale locale) {
    ConnectorHandler handler = handlerMap.get(connectorName);
    return handler.getSqoopConnector().getBundle(locale);
  }

  public MConnector getConnectorConfigurable(long connectorId) {
    String connectorName = idToNameMap.get(connectorId);
    if (connectorName == null) {
      throw new SqoopException(CommonRepositoryError.COMMON_0057, "Couldn't find"
          + " connector with id " + connectorId);
    }
    return getConnectorConfigurable(connectorName);
  }

  public MConnector getConnectorConfigurable(String connectorName) {
    ConnectorHandler handler = handlerMap.get(connectorName);
    if (handler == null) {
      throw new SqoopException(CommonRepositoryError.COMMON_0057, "Couldn't find"
          + " connector with name " + connectorName);
    }
    return handler.getConnectorConfigurable();
  }

  public SqoopConnector getSqoopConnector(long connectorId) {
    return getSqoopConnector(idToNameMap.get(connectorId));
  }

  public SqoopConnector getSqoopConnector(String uniqueName) {
    return handlerMap.get(uniqueName).getSqoopConnector();
  }

  public synchronized void initialize() {
    initialize(SqoopConfiguration.getInstance().getContext().getBoolean(ConfigurationConstants.CONNECTOR_AUTO_UPGRADE, DEFAULT_AUTO_UPGRADE));
  }

  public synchronized void initialize(boolean autoUpgrade) {
    if (handlerMap == null) {
      handlerMap = new HashMap<String, ConnectorHandler>();
    }
    if (idToNameMap == null) {
      idToNameMap = new HashMap<Long, String>();
    }
    if (connectorNames == null) {
      connectorNames = new HashSet<String>();
    }
    if (blacklistedConnectors == null) {
      String blacklistedConnectorsString =
        SqoopConfiguration.getInstance().getContext().getString(ConfigurationConstants.BLACKLISTED_CONNECTORS);

      if (blacklistedConnectorsString == null) {
        blacklistedConnectors = Collections.EMPTY_SET;
      } else {
        blacklistedConnectors = ContextUtils.getUniqueStrings(blacklistedConnectorsString);
      }
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Begin connector manager initialization");
    }

    List<URL> connectorConfigs = ConnectorManagerUtils.getConnectorConfigs(blacklistedConnectors);

    LOG.info("Connector config urls: " + connectorConfigs);

    if (connectorConfigs.size() == 0) {
      throw new SqoopException(ConnectorError.CONN_0002);
    }

    for (URL url : connectorConfigs) {
      ConnectorHandler handler = new ConnectorHandler(url);
      ConnectorHandler handlerOld =
          handlerMap.put(handler.getUniqueName(), handler);
      if (handlerOld != null) {
        throw new SqoopException(ConnectorError.CONN_0006,
            handler + ", " + handlerOld);
      }
    }

    registerConnectors(autoUpgrade);

    SqoopConfiguration.getInstance().getProvider()
        .registerListener(new CoreConfigurationListener(this));

    if (LOG.isInfoEnabled()) {
      LOG.info("Connectors loaded: " + handlerMap);
    }
  }

  private synchronized void registerConnectors(boolean autoUpgrade) {
    Repository repository = RepositoryManager.getInstance().getRepository();

    RepositoryTransaction rtx = null;
    try {
      rtx = repository.getTransaction();
      rtx.begin();
      for (Map.Entry<String, ConnectorHandler> entry : handlerMap.entrySet()) {
        ConnectorHandler handler = entry.getValue();
        MConnector newConnector = handler.getConnectorConfigurable();
        MConnector registeredConnector = repository.registerConnector(newConnector, autoUpgrade);
        // Set the registered connector in the database to the connector configurable instance
        handler.setConnectorConfigurable(registeredConnector);

        String connectorName = handler.getUniqueName();
        if (!handler.getConnectorConfigurable().hasPersistenceId()) {
          throw new SqoopException(ConnectorError.CONN_0010, connectorName);
        }
        idToNameMap.put(handler.getConnectorConfigurable().getPersistenceId(), connectorName);
        connectorNames.add(connectorName);
        LOG.debug("Registered connector: " + handler.getConnectorConfigurable());
      }
      rtx.commit();
    } catch (Exception ex) {
      if (rtx != null) {
        rtx.rollback();
      }
      throw new SqoopException(ConnectorError.CONN_0007, ex);
    } finally {
      if (rtx != null) {
        rtx.close();
      }
    }
  }

  public synchronized void destroy() {
    handlerMap = null;
    idToNameMap = null;
    connectorNames = null;
    blacklistedConnectors = null;
  }

  @Override
  public synchronized void configurationChanged() {
    LOG.info("Begin connector manager reconfiguring");
    // If there are configuration options for ConnectorManager,
    // implement the reconfiguration procedure right here.
    LOG.info("Connector manager reconfigured");
  }

}
