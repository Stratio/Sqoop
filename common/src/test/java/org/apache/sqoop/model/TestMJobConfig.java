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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestMJobConfig {
  /**
   * Test for class initialization and values
   */
  @Test
  public void testInitialization() {
    List<MConfig> configs = new ArrayList<>();
    List<MValidator> validators = new ArrayList<>();
    MFromConfig fromJobConfig = new MFromConfig(configs, validators);
    List<MConfig> configs2 = new ArrayList<>();
    List<MValidator> validators2 = new ArrayList<>();
    MFromConfig fromJobConfig2 = new MFromConfig(configs2, validators2);
    assertEquals(fromJobConfig2, fromJobConfig);
    MConfig c = new MConfig("test", null, Collections.EMPTY_LIST);
    configs2.add(c);
    assertFalse(fromJobConfig.equals(fromJobConfig2));
  }
}
