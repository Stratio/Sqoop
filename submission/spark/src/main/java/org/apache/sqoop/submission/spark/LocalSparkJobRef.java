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

import org.apache.spark.api.java.JavaSparkContext;

public class LocalSparkJobRef implements SqoopSparkJobRef {

    private final String jobId;
    private final SqoopConf sqoopConf;
    private final LocalSparkJobStatus sparkJobStatus;
    private final JavaSparkContext javaSparkContext;

    public LocalSparkJobRef(String jobId, SqoopConf sqoopConf, LocalSparkJobStatus sparkJobStatus,
            JavaSparkContext javaSparkContext) {

        this.jobId = jobId;
        this.sqoopConf = sqoopConf;
        this.sparkJobStatus = sparkJobStatus;
        this.javaSparkContext = javaSparkContext;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public SqoopSparkJobStatus getSparkJobStatus() {
        return sparkJobStatus;
    }

    @Override
    public boolean cancelJob() {
        int id = Integer.parseInt(jobId);
        javaSparkContext.sc().cancelJob(id);
        return true;
    }

}
