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
package org.apache.sqoop.shell;

import com.google.common.collect.ImmutableMap;
import org.apache.sqoop.shell.core.Constants;
import org.codehaus.groovy.tools.shell.Groovysh;

/**
 *
 */
public class CreateCommand extends SqoopCommand {

  public CreateCommand(Groovysh shell) {
    super(shell,
      Constants.CMD_CREATE,
      Constants.CMD_CREATE_SC,
      ImmutableMap.of(
        Constants.FN_LINK, CreateLinkFunction.class,
        Constants.FN_JOB, CreateJobFunction.class,
        Constants.FN_ROLE, CreateRoleFunction.class
      )
    );
  }
}
