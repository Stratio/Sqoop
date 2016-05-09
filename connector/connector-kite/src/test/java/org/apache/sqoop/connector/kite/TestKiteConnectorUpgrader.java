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
package org.apache.sqoop.connector.kite;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MValidator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;

import static org.testng.Assert.assertEquals;

public class TestKiteConnectorUpgrader {
  private KiteConnectorUpgrader upgrader;

  @BeforeMethod(alwaysRun = true)
  public void setup() {
    upgrader = new KiteConnectorUpgrader();
  }

  @Test
  public void testLinkUpgrade() throws Exception {
    MLinkConfig originalConfigs = new MLinkConfig(new LinkedList<MConfig>(), new LinkedList<MValidator>());
    MLinkConfig newConfigs = new MLinkConfig(ConfigUtils.toConfigs(LinkConfiguration.class), ConfigUtils.getMValidatorsFromConfigurationClass(LinkConfiguration.class));
    originalConfigs.getConfigs().add(new MConfig("linkConfig", new LinkedList<MInput<?>>(), Collections.EMPTY_LIST));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("linkConfig.hdfsHostAndPort", false, InputEditable.ANY, StringUtils.EMPTY, (short)255, Collections.EMPTY_LIST));
    originalConfigs.getInput("linkConfig.hdfsHostAndPort").setValue("test:8020");
    upgrader.upgradeLinkConfig(originalConfigs, newConfigs);
    assertEquals("test:8020", newConfigs.getInput("linkConfig.authority").getValue());
  }
}