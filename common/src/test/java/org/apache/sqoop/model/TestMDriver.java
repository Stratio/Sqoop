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
package org.apache.sqoop.model;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.sqoop.json.DriverBean;
import org.testng.annotations.Test;

public class TestMDriver {

  @Test
  public void testDriver() {
    List<MConfig> driverConfig = new ArrayList<>();
    driverConfig.add(new MConfig("driver-test", new ArrayList<MInput<?>>(), Collections.EMPTY_LIST));

    List<MValidator> driverValidators = new ArrayList<>();
    driverValidators.add(new MValidator("test", ""));

    MDriverConfig mDriverConfig = new MDriverConfig(driverConfig, driverValidators);

    MDriver driver = new MDriver(mDriverConfig, DriverBean.CURRENT_DRIVER_VERSION);
    assertEquals(1, driver.getDriverConfig().getConfigs().size());
    assertEquals("driver-test", driver.getDriverConfig().getConfigs().get(0).getName());
    assertEquals("test", driver.getDriverConfig().getValidators().get(0).getValidatorClass());
    assertEquals("", driver.getDriverConfig().getValidators().get(0).getStrArg());
  }
}
