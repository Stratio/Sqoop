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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.job.SparkJobConstants;
import org.apache.sqoop.job.spark.SparkDestroyerExecutor;

public class LocalSqoopSparkClient extends SqoopSparkClientManager {

    private static final long serialVersionUID = 1L;
    protected static final transient Log LOG = LogFactory.getLog(LocalSqoopSparkClient.class);

    private static LocalSqoopSparkClient client;

    public static synchronized LocalSqoopSparkClient getInstance(SparkConf sparkConf) {
        if (client == null) {
            client = new LocalSqoopSparkClient(sparkConf);
        }
        return client;
    }

    public LocalSqoopSparkClient(SparkConf sparkConf) {
        context = new JavaSparkContext(sparkConf);
        String fatJar=sparkConf.get("spark.jars");
        context.addJar(fatJar);
    }

    public SparkConf getSparkConf() {
        return context.getConf();
    }
    public JavaSparkContext getSparkContext() {
        return context;
    }

    public int getExecutorCount() {
        return context.sc().getExecutorMemoryStatus().size();
    }

    public int getDefaultParallelism() throws Exception {
        return context.sc().defaultParallelism();
    }

    public void execute(JobRequest request) throws Exception {
        SqoopSparkDriver.execute(request, getSparkConf(), context);
        SparkDestroyerExecutor.executeDestroyer(true, request, Direction.FROM, SparkJobConstants.SUBMITTING_USER);
        SparkDestroyerExecutor.executeDestroyer(true, request, Direction.TO,SparkJobConstants.SUBMITTING_USER);
        request.getJobSubmission().setExternalJobId(request.getJobName());

    }

    public void stop(String jobId) throws Exception {
        context.cancelJobGroup(jobId);
        client = null;
    }

    @Override
    public void close() throws IOException {

    }
}
