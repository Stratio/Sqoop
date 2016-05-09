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


import java.io.Closeable;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.driver.JobRequest;

public interface SqoopSparkClient extends Serializable,Closeable {

    void execute(JobRequest request) throws Exception;

    void stop(String jobId) throws Exception;


    /**
     * @return spark configuration
     */
    SparkConf getSparkConf();
    /**
     * @return spark configuration
     */
    JavaSparkContext getSparkContext();
    /**
     * @return the number of executors
     */
    int getExecutorCount() throws Exception;

    /**
     * For standalone mode, this can be used to get total number of cores.
     * @return  default parallelism.
     */
    int getDefaultParallelism() throws Exception;
}
