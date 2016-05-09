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
/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.sqoop.common.test.kafka;

import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;

/**
 * A utility class for starting/stopping Kafka Server.
 */
public class TestUtil {

  private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);
  private static TestUtil instance = new TestUtil();

  private KafkaRunnerBase kafkaServer;
  private KafkaConsumer kafkaConsumer;

  private TestUtil() {}

  public static TestUtil getInstance() {
    return instance;
  }

  private boolean startKafkaServer() throws IOException, InterruptedException, ClassNotFoundException,
                                            IllegalAccessException, InstantiationException {
    kafkaServer = KafkaRunnerFactory.getKafkaRunner();

    try {
      kafkaServer.start();
    } catch (Exception e) {
      logger.error("Error starting the Kafka Server.", e);
      return false;
    }

    return true;
  }


  private KafkaConsumer getKafkaConsumer() {
    synchronized (this) {
      if (kafkaConsumer == null) {
        kafkaConsumer = new KafkaConsumer();
      }
    }
    return kafkaConsumer;
  }

  public void initTopicList(List<String> topics) {
    getKafkaConsumer().initTopicList(topics);
  }

  public MessageAndMetadata getNextMessageFromConsumer(String topic) {
    return getKafkaConsumer().getNextMessage(topic);
  }

  public void prepare() throws Exception {
    boolean startStatus = startKafkaServer();
    if (!startStatus) {
      throw new RuntimeException("Error starting the server!");
    }
    try {
      Thread.sleep(3 * 1000);   // add this sleep time to
      // ensure that the server is fully started before proceeding with tests.
    } catch (InterruptedException e) {
      // ignore
    }
    getKafkaConsumer();
    logger.info("Completed the prepare phase.");
  }

  public void tearDown() throws IOException {
    logger.info("Shutting down the Kafka Consumer.");

    // Set kafkaConsumer to null so that a new one is created when
    // getKafkaConsumer is called.
    if (kafkaConsumer != null) {
      kafkaConsumer.shutdown();
      kafkaConsumer = null;
    }

    try {
      Thread.sleep(3 * 1000);   // add this sleep time to
      // ensure that the server is fully started before proceeding with tests.
    } catch (InterruptedException e) {
      // ignore
    }
    if (kafkaServer != null) {
      logger.info("Shutting down the kafka Server.");
      kafkaServer.stop();
      logger.info("Completed the tearDown phase.");
    } else {
      logger.info("Kafka Server haven't been started, skipping shutdown");
    }
  }

  public String getZkUrl() {
    return kafkaServer.getZkConnectionString();
  }

  public String getKafkaServerUrl() {
    return kafkaServer.getKafkaUrl();
  }
}
