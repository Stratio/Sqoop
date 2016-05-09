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


import org.apache.sqoop.json.JsonBean;

public interface RequestHandler extends java.io.Serializable {

  static final String CONNECTOR_NAME_QUERY_PARAM = "cname";
  static final String JOB_NAME_QUERY_PARAM = "jname";
  static final String ROLE_NAME_QUERY_PARAM = "role_name";
  static final String PRINCIPAL_NAME_QUERY_PARAM = "principal_name";
  static final String PRINCIPAL_TYPE_QUERY_PARAM = "principal_type";
  static final String RESOURCE_NAME_QUERY_PARAM = "resource_name";
  static final String RESOURCE_TYPE_QUERY_PARAM = "resource_type";

  JsonBean handleEvent(RequestContext ctx);
}
