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

package org.apache.sqoop.test.minicluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.sqoop.server.SqoopJettyServer;

/**
* Embedded jetty Sqoop server mini cluster.
*
* This mini cluster will start up embedded jetty
*/
public class JettySqoopMiniCluster extends SqoopMiniCluster {

  private SqoopJettyServer sqoopJettyServer;

  /** {@inheritDoc} */
  public JettySqoopMiniCluster(String temporaryPath, Configuration configuration) throws Exception {
    super(temporaryPath, configuration);
  }

  @Override
  public void start() throws Exception {
    prepareTemporaryPath();
    sqoopJettyServer = new SqoopJettyServer();
    sqoopJettyServer.startServer();
  }

  /** {@inheritDoc} */
  @Override
  public void stop() throws Exception {
    if (sqoopJettyServer != null) {
      sqoopJettyServer.stopServerForTest();
    }
  }

  /**
  * Return server URL.
  */
  @Override
  public String getServerUrl() {
    if (sqoopJettyServer != null) {
      return sqoopJettyServer.getServerUrl();
    }
    throw new RuntimeException("Jetty server wasn't started.");
  }
}
