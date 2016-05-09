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
package org.apache.sqoop.connector.ftp;

import java.io.Serializable;

import org.apache.sqoop.configurable.ConfigurableUpgradeUtil;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;

//NOTE: All config types have the similar upgrade path at this point
public class FtpConnectorUpgrader extends ConnectorConfigurableUpgrader implements Serializable {

  /**
   * {@inheritDoc}
   */
  @Override
  public void upgradeLinkConfig(MLinkConfig original, MLinkConfig upgradeTarget) {
    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(),
                                      upgradeTarget.getConfigs());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void upgradeToJobConfig(MToConfig original, MToConfig upgradeTarget) {
    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(),
                                      upgradeTarget.getConfigs());
  }
}
