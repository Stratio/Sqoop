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
package org.apache.sqoop.connector.kafka;

import org.apache.sqoop.connector.kafka.configuration.LinkConfiguration;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.ConfigValidationRunner;
import org.apache.sqoop.validation.Status;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestConfiguration {
  @Test
  public void testValidURI() {
    String[] brokerList = {
            "broker1:9092",
            "broker1:9092,broker2:9092"
    };

    String[] zkList = {
        "zk1:2181/kafka",
        "zk1:2181,zk2:2181/kafka"
    };

    ConfigValidationRunner runner = new ConfigValidationRunner();
    LinkConfiguration linkConfiguration;
    ConfigValidationResult result;

    for (String brokerURI : brokerList) {
      for (String zkURI : zkList) {
        linkConfiguration = new LinkConfiguration();
        linkConfiguration.linkConfig.brokerList = brokerURI;
        linkConfiguration.linkConfig.zookeeperConnect = zkURI;

        result = runner.validate(linkConfiguration);
        assertEquals(Status.OK, result.getStatus());
      }
    }
  }

  @Test
  public void testInvalidURI() {
    String[] brokerList = {
        "",
        "broker",
        "broker1:9092,broker"
    };

    String[] zkList = {
        "zk1:2181/kafka",
        "zk1:2181,zk2:2181/kafka"
    };

    ConfigValidationRunner runner = new ConfigValidationRunner();
    LinkConfiguration linkConfiguration;
    ConfigValidationResult result;

    for (String brokerURI : brokerList) {
      for (String zkURI : zkList) {
        linkConfiguration = new LinkConfiguration();
        linkConfiguration.linkConfig.brokerList = brokerURI;
        linkConfiguration.linkConfig.zookeeperConnect = zkURI;

        result = runner.validate(linkConfiguration);
        assertEquals(Status.ERROR, result.getStatus());
      }
    }
  }
}
