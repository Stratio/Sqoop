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
package org.apache.sqoop.json;

import java.util.List;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.model.MLink;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LinksBean extends LinkBean {

  static final String LINKS = "links";

  public LinksBean(MLink link) {
    super(link);
  }

  public LinksBean(List<MLink> links) {
    super(links);
  }

  // For "restore"
  public LinksBean() {

  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract(boolean skipSensitive) {
    JSONArray linkArray = extractLinks(skipSensitive);
    JSONObject links = new JSONObject();
    links.put(LINKS, linkArray);
    return links;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    JSONArray array = JSONUtils.getJSONArray(jsonObject, LINKS);
    super.restoreLinks(array);
  }
}