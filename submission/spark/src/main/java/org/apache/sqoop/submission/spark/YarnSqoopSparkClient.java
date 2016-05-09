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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.job.SparkJobConstants;
import org.apache.sqoop.job.spark.SparkDestroyerExecutor;

public class YarnSqoopSparkClient extends SqoopSparkClientManager {

    private static final long serialVersionUID = 1L;
    protected static final transient Log LOG = LogFactory.getLog(YarnSqoopSparkClient.class);

    protected static String YARN_MODE= "SPARK_YARN_MODE";
    private static YarnSqoopSparkClient client;

    private static YarnConfiguration yarnConfig;

    private static String[] args;

    private static ClientArguments clientArgs;

    public static synchronized YarnSqoopSparkClient getInstance(Map<String, String> conf) {
        if (client == null) {
            System.setProperty(YARN_MODE, "true");

            SparkConf sparkConf= SqoopSparkClientFactory.generateSparkConf(conf);

            yarnConfig= generateYarnSparkConf(conf);
            client = new YarnSqoopSparkClient(sparkConf);

            clientArgs = new ClientArguments(args, sparkConf);

        }
        return client;
    }

    static YarnConfiguration generateYarnSparkConf(Map<String, String> conf) {
        YarnConfiguration yarnConf = new YarnConfiguration();
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            yarnConf.set(entry.getKey(), entry.getValue());
        }
        return yarnConf;
    }

    public YarnSqoopSparkClient(SparkConf sparkConf) {
        context = new JavaSparkContext(sparkConf);
    }


    public void execute(JobRequest request) throws Exception {

        request.getJobSubmission().setExternalJobId(String.valueOf(request.getJobId()));
        request.getJobSubmission().setExternalLink(request.getNotificationUrl());

        Client yarnClient= new Client(clientArgs, yarnConfig, getSparkConf());
        yarnClient.run();

//      SparkCounters sparkCounters = new SparkCounters(sc);
        SqoopSparkDriver.execute(request, getSparkConf(), context);
        SparkDestroyerExecutor.executeDestroyer(true, request, Direction.FROM, SparkJobConstants.SUBMITTING_USER);
        SparkDestroyerExecutor.executeDestroyer(true, request, Direction.TO,SparkJobConstants.SUBMITTING_USER);

    }

    public void stop(String jobId) throws Exception {
        context.stop();
        client = null;
    }

    @Override
    public JavaSparkContext getSparkContext() {
        return context;
    }

    @Override
    public void close() throws IOException {

    }
}
