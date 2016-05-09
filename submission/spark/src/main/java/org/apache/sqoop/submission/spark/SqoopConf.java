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
package org.apache.sqoop.submission.spark;

import java.util.HashMap;
import java.util.Map;


// contains all the required confs for sqoop-spark
public class SqoopConf {

    private Map<String, String> props = new HashMap<String, String>();

    SqoopConf() {

    }

    public Map<String, String> getProps() {
        return props;
    }

    public void add(String key, String value){
        props.put(key, value);
    }

    public String get(String key) {
        return props.get(key);
    }
}
