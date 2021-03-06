/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.json.util;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigType;
import org.apache.sqoop.model.MDateTimeInput;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MListInput;
import org.apache.sqoop.model.MLongInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MValidator;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

/**
 *
 */
public class TestConfigSerialization {

  @Test
  public void testAllDataTypes() {
    // Inserted values
    Map<String, String> map = new HashMap<String, String>();
    map.put("A", "B");

    // Fill config with all values
    MConfig config = getConfig();
    config.getStringInput("String").setValue("A");
    config.getMapInput("Map").setValue(map);
    config.getIntegerInput("Integer").setValue(1);
    config.getBooleanInput("Boolean").setValue(true);
    config.getEnumInput("Enum").setValue("YES");

    // Serialize that into JSON
    JSONObject jsonObject = ConfigInputSerialization.extractConfig(config, MConfigType.JOB,  false);
    assertNotNull(jsonObject);

    // Exchange the data on string level
    String serializedJson = jsonObject.toJSONString();
    JSONObject retrievedJson = JSONUtils.parse(serializedJson);

    // And retrieve back from JSON representation
    MConfig retrieved = ConfigInputSerialization.restoreConfig(retrievedJson);

    // Verify all expected values
    assertEquals("A", retrieved.getStringInput("String").getValue());
    assertEquals(map, retrieved.getMapInput("Map").getValue());
    assertEquals(1, (int)retrieved.getIntegerInput("Integer").getValue());
    assertEquals(true, retrieved.getBooleanInput("Boolean").getValue().booleanValue());
    assertEquals("YES", retrieved.getEnumInput("Enum").getValue());

    // Verify all expected input validators
    assertEquals(Collections.EMPTY_LIST, retrieved.getStringInput("String")
      .getValidators());
    assertEquals(buildMValidators(), retrieved.getMapInput("Map")
      .getValidators());
    assertEquals(Collections.EMPTY_LIST, retrieved.getIntegerInput("Integer").getValidators());
    assertEquals(Collections.EMPTY_LIST, retrieved.getBooleanInput("Boolean").getValidators());
    assertEquals(Collections.EMPTY_LIST, retrieved.getEnumInput("Enum").getValidators());

    // Verify config validators
    assertEquals(buildMValidators(), config.getValidators());
  }

  @Test
  public void testMapDataType() {
    MConfig config = getMapConfig();

    // Inserted values
    Map<String, String> map = new HashMap<String, String>();
    map.put("A", "B");
    config.getMapInput("Map").setValue(map);

    // Serialize
    JSONObject jsonObject = ConfigInputSerialization.extractConfig(config, MConfigType.JOB, false);
    String serializedJson = jsonObject.toJSONString();

    // Deserialize
    JSONObject retrievedJson = JSONUtils.parse(serializedJson);
    MConfig retrieved = ConfigInputSerialization.restoreConfig(retrievedJson);
    assertEquals(map, retrieved.getMapInput("Map").getValue());
  }

  @Test(expectedExceptions=SqoopException.class)
  public void testMapDataTypeException() {
    MConfig config = getMapConfig();

    // Inserted values
    Map<String, String> map = new HashMap<String, String>();
    map.put("A", "B");
    config.getMapInput("Map").setValue(map);

    // Serialize
    JSONObject jsonObject = ConfigInputSerialization.extractConfig(config, MConfigType.JOB, false);
    String serializedJson = jsonObject.toJSONString();

    // Replace map value with a fake string to force exception
    String badSerializedJson = serializedJson.replace("{\"A\":\"B\"}",
      "\"nonsensical string\"");
    System.out.println(badSerializedJson);
    JSONObject retrievedJson = JSONUtils.parse(badSerializedJson);
    ConfigInputSerialization.restoreConfig(retrievedJson);
  }

  @Test
  public void testMapDataTypeSensitiveKeys() {
    MConfig config = getMapConfig();

    // Inserted values
    Map<String, String> map = new HashMap<String, String>();
    map.put("A", "B");
    config.getMapInput("Map").setValue(map);

    // Serialize
    JSONObject jsonObject = ConfigInputSerialization.extractConfig(config, MConfigType.JOB, true);
    String serializedJson = jsonObject.toJSONString();

    // Map with sensitive values redacted
    Map<String, String> sensitiveMap = new HashMap<String, String>();
    sensitiveMap.put("A", MMapInput.SENSITIVE_VALUE_PLACEHOLDER);

    // Deserialize
    JSONObject retrievedJson = JSONUtils.parse(serializedJson);
    MConfig retrieved = ConfigInputSerialization.restoreConfig(retrievedJson);
    assertEquals(sensitiveMap, retrieved.getMapInput("Map").getValue());
  }

  @Test
  public void testInputEditableOptional() {
    // Inserted values
    Map<String, String> map = new HashMap<String, String>();
    map.put("A", "B");

    List<String> list = new LinkedList<String>();
    list.add("C");

    DateTime dt = new DateTime(12345678L);

    // Fill config with all values
    MConfig config = getConfig();
    config.getStringInput("String").setValue("A");
    config.getMapInput("Map").setValue(map);
    config.getIntegerInput("Integer").setValue(1);
    config.getBooleanInput("Boolean").setValue(true);
    config.getEnumInput("Enum").setValue("YES");
    config.getListInput("List").setValue(list);
    config.getDateTimeInput("DateTime").setValue(dt);

    // Serialize that into JSON
    JSONObject jsonObject = ConfigInputSerialization.extractConfig(config, MConfigType.JOB,  false);
    assertNotNull(jsonObject);

    // Make sure editable is optional
    // Remove the editable
    JSONArray inputs = (JSONArray) jsonObject.get(ConfigInputConstants.CONFIG_INPUTS);
    for (int i = 0; i < inputs.size(); i++) {
      JSONObject input = (JSONObject) inputs.get(i);
      if ((input.containsKey(ConfigInputConstants.CONFIG_INPUT_EDITABLE))) {
        input.remove(ConfigInputConstants.CONFIG_INPUT_EDITABLE);
      }
    }

    // Exchange the data on string level
    String serializedJson = jsonObject.toJSONString();
    JSONObject retrievedJson = JSONUtils.parse(serializedJson);

    // Make sure editable isn't part of the JSON
    inputs = (JSONArray) retrievedJson.get(ConfigInputConstants.CONFIG_INPUTS);
    for (int i = 0; i < inputs.size(); i++) {
      JSONObject input = (JSONObject) inputs.get(i);
      assertFalse(input.containsKey(ConfigInputConstants.CONFIG_INPUT_EDITABLE));
    }

    // And retrieve back from JSON representation
    MConfig retrieved = ConfigInputSerialization.restoreConfig(retrievedJson);

    // Verify all expected values
    assertEquals("A", retrieved.getStringInput("String").getValue());
    assertEquals(map, retrieved.getMapInput("Map").getValue());
    assertEquals(1, (int)retrieved.getIntegerInput("Integer").getValue());
    assertEquals(true, retrieved.getBooleanInput("Boolean").getValue().booleanValue());
    assertEquals("YES", retrieved.getEnumInput("Enum").getValue());
    assertEquals(list, retrieved.getListInput("List").getValue());
    assertEquals(dt, retrieved.getDateTimeInput("DateTime").getValue());

    // Verify all expected input validators
    assertEquals(Collections.EMPTY_LIST, retrieved.getStringInput("String").getValidators());
    assertEquals(buildMValidators(), retrieved.getMapInput("Map").getValidators());
    assertEquals(Collections.EMPTY_LIST, retrieved.getIntegerInput("Integer").getValidators());
    assertEquals(Collections.EMPTY_LIST, retrieved.getBooleanInput("Boolean").getValidators());
    assertEquals(Collections.EMPTY_LIST, retrieved.getEnumInput("Enum").getValidators());
    assertEquals(Collections.EMPTY_LIST, retrieved.getListInput("List").getValidators());
    assertEquals(buildMValidators(), retrieved.getDateTimeInput("DateTime").getValidators());

    // Verify config validators
    assertEquals(buildMValidators(), config.getValidators());
  }

  protected MConfig getMapConfig() {
    List<MInput<?>> inputs;
    MInput input;

    inputs = new LinkedList<MInput<?>>();

    input = new MMapInput("Map", false, InputEditable.ANY, StringUtils.EMPTY, "A", Collections.EMPTY_LIST);
    inputs.add(input);

    return new MConfig("c", inputs, Collections.EMPTY_LIST);
  }

  /**
   * Return config with all data types.
   *
   * @return
   */
  protected MConfig getConfig() {
    List<MInput<?>> inputs;
    MInput<?> input;

    List<MValidator> mValidatorsForInput = buildMValidators();

    inputs = new LinkedList<MInput<?>>();

    input = new MStringInput("String", false, InputEditable.ANY, StringUtils.EMPTY, (short)30, Collections.EMPTY_LIST);
    inputs.add(input);

    input = new MMapInput("Map", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, mValidatorsForInput);
    inputs.add(input);

    input = new MIntegerInput("Integer", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    inputs.add(input);

    input = new MLongInput("Long", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    inputs.add(input);

    input = new MBooleanInput("Boolean", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    inputs.add(input);

    input = new MEnumInput("Enum", false, InputEditable.ANY, StringUtils.EMPTY, new String[] {"YES", "NO"}, Collections.EMPTY_LIST);
    inputs.add(input);

    input = new MListInput("List", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    inputs.add(input);

    input = new MDateTimeInput("DateTime", false, InputEditable.ANY, StringUtils.EMPTY, mValidatorsForInput);
    inputs.add(input);

    List<MValidator> mValidatorsForConfig = buildMValidators();

    return new MConfig("c", inputs, mValidatorsForConfig);
  }

  protected List<MValidator> buildMValidators() {
    List<MValidator> mValidators = new ArrayList<>();
    mValidators.add(new MValidator("validator1", AbstractValidator.DEFAULT_STRING_ARGUMENT));
    mValidators.add(new MValidator("validator1", "strarg"));
    return mValidators;
  }
}
