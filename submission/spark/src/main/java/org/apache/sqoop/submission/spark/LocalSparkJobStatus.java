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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.common.SqoopException;

public class LocalSparkJobStatus implements SqoopSparkJobStatus {

    private final JavaSparkContext sparkContext;
    private static final Log LOG = LogFactory.getLog(LocalSparkJobStatus.class
            .getName());
    private int jobId;
    private JavaFutureAction<Void> future;

    public LocalSparkJobStatus(JavaSparkContext sparkContext, int jobId,
            JavaFutureAction<Void> future) {
        this.sparkContext = sparkContext;
        this.jobId = jobId;
        this.future = future;
    }

    @Override
    public int getJobId() {
        return jobId;
    }

    @Override
    public JobExecutionStatus getState() {
        SparkJobInfo sparkJobInfo = getJobInfo();
        // For spark job with empty source data, it's not submitted actually, so
        // we would never
        // receive JobStart/JobEnd event in JobStateListener, use
        // JavaFutureAction to get current
        // job state.

        if (sparkJobInfo == null && future.isDone()) {
            try {
                future.get();
            } catch (Exception e) {
                LOG.error("Failed to run job " + jobId, e);
                return JobExecutionStatus.FAILED;
            }
            return JobExecutionStatus.SUCCEEDED;
        }
        return sparkJobInfo == null ? null : sparkJobInfo.status();
    }

    @Override
    public JobExecutionStatus getStatus() throws SqoopException {
        SparkJobInfo sparkJobInfo = getJobInfo();
        return sparkJobInfo.status();
    }

    @Override
    public void cleanup() {
    }

    private SparkJobInfo getJobInfo() {
        return sparkContext.statusTracker().getJobInfo(jobId);
    }

}
