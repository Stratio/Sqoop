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

import org.apache.sqoop.connector.ftp.configuration.LinkConfiguration;
import org.apache.sqoop.connector.ftp.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;

/**
 * Perform any required initialization before execution of job.
 */
public class FtpToInitializer extends Initializer<LinkConfiguration, ToJobConfiguration> implements Serializable {

  /**
   * Initialize new submission based on given configuration properties. Any
   * needed temporary values might be saved to context object and they will be
   * promoted to all other part of the workflow automatically.
   *
   * @param context Initializer context object
   * @param linkConfig link configuration object
   * @param jobConfig TO job configuration object
   */
  @Override
  public void initialize(InitializerContext context, LinkConfiguration linkConfig,
      ToJobConfiguration jobConfig) {
    // do nothing at this point
  }
}
