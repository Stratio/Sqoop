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
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;

import java.util.UUID;

public class TestLoader extends Loader<TestLinkConfiguration, TestToJobConfiguration> {

  private long rowsWritten = 0;


  @Override
  public void load(LoaderContext context, TestLinkConfiguration linkConfiguration,
                   TestToJobConfiguration toJobConfig) throws Exception {
    DataReader reader = context.getDataReader();
    //This will break if the TestDependency jar is not loaded
    TestDependency testDependency = new TestDependency();
    while (reader.readTextRecord() != null){
      rowsWritten++;
    }
  }

  @Override
  public long getRowsWritten() {
    return rowsWritten;
  }
}