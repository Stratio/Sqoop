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

import java.util.Locale;

/**
 * Model describing entire privilege object which used in privilege based authorization controller
 */
public class MPrivilege {

  public static enum ACTION {ALL, READ, WRITE}

  private final MResource resource;
  /**
   * Currently, the action supports view, use, create, update, delete and enable_disable.
   */
  private final ACTION action;
  private final boolean with_grant_option;


  /**
   * Default constructor to build  new MPrivilege model.
   *
   * @param resource          Privilege resource
   * @param action            Privilege action
   * @param with_grant_option Privilege with_grant_option
   */
  public MPrivilege(MResource resource,
                    ACTION action,
                    boolean with_grant_option) {
    this.resource = resource;
    this.action = action;
    this.with_grant_option = with_grant_option;
  }

  /**
   * constructor to build  new MPrivilege model.
   *
   * @param resource          Privilege resource
   * @param actionName        Privilege action name
   * @param with_grant_option Privilege with_grant_option
   */
  public MPrivilege(MResource resource,
                    String actionName,
                    boolean with_grant_option) {
    this(resource, ACTION.valueOf(actionName.toUpperCase(Locale.getDefault())), with_grant_option);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Privilege (");
    sb.append("Privilege resource: ").append(this.getResource().toString());
    sb.append(", Privilege action: ").append(this.action);
    sb.append(", Privilege with_grant_option: ").append(this.with_grant_option);
    sb.append(" )");

    return sb.toString();
  }

  public MResource getResource() {
    return resource;
  }

  public String getAction() {
    return action.name();
  }

  public boolean isWith_grant_option() {
    return with_grant_option;
  }
}
