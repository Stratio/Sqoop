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
package org.apache.sqoop.server;

import org.apache.log4j.Logger;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.core.SqoopServer;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Initializes the Sqoop server. This listener is also responsible for
 * cleaning up any resources occupied by the server during the system shutdown.
 */
public class ServerInitializer implements ServletContextListener {

  private static final Logger LOG = Logger.getLogger(ServerInitializer.class);

  public void contextDestroyed(ServletContextEvent arg0) {
    SqoopServer.destroy();
  }

  public void contextInitialized(ServletContextEvent arg0) {
    try {
      SqoopServer.initialize();
    } catch (Throwable ex) {
      throw new RuntimeException("Sqoop server failed to start.", ex);
    }
  }
}
