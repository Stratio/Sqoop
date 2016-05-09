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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.driver.JobRequest;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

public abstract class SqoopSparkClientManager implements SqoopSparkClient {

    protected static final Splitter CSV_SPLITTER = Splitter.on(",").omitEmptyStrings();

    protected JavaSparkContext context;

    protected List<String> localJars = new ArrayList<String>();

    protected List<String> localFiles = new ArrayList<String>();

    @Override public void execute(JobRequest request) throws Exception {

    }

    public SparkConf getSparkConf() {
        return context.getConf();
    }

    public int getExecutorCount() {
        return context.sc().getExecutorMemoryStatus().size();
    }

    public int getDefaultParallelism() throws Exception {
        return context.sc().defaultParallelism();
    }


    public void addResources(String addedFiles) {
        for (String addedFile : CSV_SPLITTER.split(Strings.nullToEmpty(addedFiles))) {
            if (!localFiles.contains(addedFile)) {
                localFiles.add(addedFile);
                context.addFile(addedFile);
            }
        }
    }

    private void addJars(String addedJars) {
        for (String addedJar : CSV_SPLITTER.split(Strings.nullToEmpty(addedJars))) {
            if (!localJars.contains(addedJar)) {
                localJars.add(addedJar);
                context.addJar(addedJar);
            }
        }
    }

}
