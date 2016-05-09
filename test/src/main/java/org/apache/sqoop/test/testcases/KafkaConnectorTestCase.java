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
package org.apache.sqoop.test.testcases;

import kafka.message.MessageAndMetadata;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.testng.Assert;
import org.apache.sqoop.common.test.kafka.TestUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import static org.apache.sqoop.connector.common.SqoopIDFUtils.toText;

public class KafkaConnectorTestCase extends ConnectorTestCase {
  private static TestUtil testUtil = TestUtil.getInstance();
  protected String topic;

  @BeforeClass(alwaysRun = true)
  public void startKafka() throws Exception {
    // starts Kafka server and its dependent zookeeper
    testUtil.prepare();
  }

  @AfterClass(alwaysRun = true)
  public void stopKafka() throws IOException {
    testUtil.tearDown();
  }

  protected void fillKafkaLinkConfig(MLink link) {
    MConfigList configs = link.getConnectorLinkConfig();
    configs.getStringInput("linkConfig.brokerList").setValue(testUtil.getKafkaServerUrl());
    configs.getStringInput("linkConfig.zookeeperConnect").setValue(testUtil.getZkUrl());

  }

  protected void fillKafkaToConfig(MJob job){
    MConfigList toConfig = job.getToJobConfig();
    toConfig.getStringInput("toJobConfig.topic").setValue(topic);
    List<String> topics = new ArrayList<String>(1);
    topics.add(topic);
    testUtil.initTopicList(topics);
  }

  /**
   * Compare strings in content to the messages in Kafka topic
   * @param content
   * @throws UnsupportedEncodingException
   */
  protected void validateContent(String[] content) throws UnsupportedEncodingException {

    Set<String> inputSet = new HashSet<String>(Arrays.asList(content));
    Set<String> outputSet = new HashSet<String>();

    for(String str: content) {
      MessageAndMetadata<byte[],byte[]> fetchedMsg =
              testUtil.getNextMessageFromConsumer(topic);
      outputSet.add(toText(new String(fetchedMsg.message(), "UTF-8")));
    }

    Assert.assertEquals(inputSet, outputSet);
  }
}
