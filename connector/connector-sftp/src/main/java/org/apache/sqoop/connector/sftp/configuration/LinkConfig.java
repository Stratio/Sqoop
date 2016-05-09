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
package org.apache.sqoop.connector.sftp.configuration;

import java.io.Serializable;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.sftp.sftpclient.SftpConnectorClient;
import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.apache.sqoop.validation.validators.NotEmpty;

/**
 * Attributes for SFTP connector link configuration.
 */
@ConfigClass(validators = {@Validator(LinkConfig.ConfigValidator.class)})
public class LinkConfig implements Serializable {

  /**
   * FTP server hostname.
   */
  @Input(size = 255, validators = {@Validator(NotEmpty.class)})
  public String server;

  /**
   * FTP server port. Default is port 22.
   */
  @Input
  public Integer port;

  /**
   * Username for server login.
   */
  @Input(size = 256, validators = {@Validator(NotEmpty.class)})
  public String username;

  /**
   * Password for server login.
   */
  @Input(size = 256, sensitive = true)
  public String password;

  /**
   * Validate that we can log into the server using the supplied credentials.
   */
  public static class ConfigValidator extends AbstractValidator<LinkConfig> {
    @Override
    public void validate(LinkConfig linkConfig) {
      try {
        SftpConnectorClient client =
          new SftpConnectorClient();
        client.connect(linkConfig.server, linkConfig.port,
                       linkConfig.username, linkConfig.password);
        client.disconnect();
      } catch (SqoopException e) {
        addMessage(Status.WARNING, "Can't connect to the SFTP server " +
                   linkConfig.server + " error is " + e.getMessage());
      }
    }
  }
}
