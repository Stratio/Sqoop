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
package org.apache.sqoop.integration.repository.derby.upgrade;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * This version contains the following structures:
 * Generic JDBC Connector link with name "Link1" and id 1
 * Generic JDBC Connector link with blank name and id 2
 * HDFS Connector link with name "Link3" and id 3
 * HDFS Connector link with blank name and id 4
 * HDFS Connector link with blank name and id 5
 * HDFS Connector link with blank name and id 6
 * Job (-f 1 -t 3) with name "Import" and id 1
 * Job (-f 1 -t 3) with name "Query" and id 2
 * Job (-f 3 -t 1) with name "Export" and id 3
 * Job (-f 3 -t 1) with blank name and id 4
 * Job (-f 3 -t 1) with blank name and id 5
 * Job (-f 1 -t 1) with name "SameConnector" and id 6
 * Job with id 1 has been executed 3 times
 * Job with id 2 has been executed 3 times
 * Job with id 3 has been executed 1 times
 * Link with id 4 has been disabled
 * Link with id 5 has been disabled
 * Job with id 4 has been disabled
 * Job with id 5 has been disabled
 */
@Test(groups = "no-real-cluster")
public class Derby1_99_4UpgradeTest extends DerbyRepositoryUpgradeTest {

  @Override
  public String getPathToRepositoryTarball() {
    return "/repository/derby/derby-repository-1.99.4.tar.gz";
  }

  @Override
  public int getNumberOfLinks() {
    return 6;
  }

  @Override
  public int getNumberOfJobs() {
    return 6;
  }

  @Override
  public Map<Integer, Integer> getNumberOfSubmissions() {
    HashMap<Integer, Integer> ret = new HashMap<Integer, Integer>();
    ret.put(1, 3);
    ret.put(2, 3);
    ret.put(3, 1);
    ret.put(4, 0);
    ret.put(5, 0);
    return ret;
  }

  @Override
  public String[] getDisabledLinkNames() {
    return new String[] {linkIdToNameMap.get(4L), linkIdToNameMap.get(5L)};
  }

  @Override
  public String[] getDisabledJobNames() {
    return new String[] {jobIdToNameMap.get(4L), jobIdToNameMap.get(5L)};
  }

  @Override
  public String[] getDeleteLinkNames() {
    return new String[] {linkIdToNameMap.get(1L), linkIdToNameMap.get(2L),
            linkIdToNameMap.get(3L), linkIdToNameMap.get(4L), linkIdToNameMap.get(5L), linkIdToNameMap.get(6L)};
  }
}
