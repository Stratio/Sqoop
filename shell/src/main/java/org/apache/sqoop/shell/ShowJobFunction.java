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
package org.apache.sqoop.shell;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.TableDisplayer;
import org.apache.sqoop.validation.Status;

import java.text.DateFormat;
import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.ConfigDisplayer.*;

/**
 *
 */
@SuppressWarnings("serial")
public class ShowJobFunction extends SqoopFunction {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("static-access")
  public ShowJobFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_JOBS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_CONNECTOR_NAME)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_JOBS_CN))
        .withLongOpt(Constants.OPT_CONNECTOR_NAME)
        .create(Constants.OPT_CONNECTOR_NAME_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_NAME)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_JOB_NAME))
        .withLongOpt(Constants.OPT_NAME)
        .create(Constants.OPT_NAME_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_ALL)) {
      showJobs(null);
    } else if (line.hasOption(Constants.OPT_CONNECTOR_NAME)) {
      showJobs(line.getOptionValue(Constants.OPT_CONNECTOR_NAME));
    } else if (line.hasOption(Constants.OPT_NAME)) {
      showJob(line.getOptionValue(Constants.OPT_NAME));
    } else {
      showSummary();
    }
    return Status.OK;
  }

  private void showSummary() {
    List<MJob> jobs = client.getJobs();

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_ID));
    header.add(resourceString(Constants.RES_TABLE_HEADER_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_FROM_CONNECTOR));
    header.add(resourceString(Constants.RES_TABLE_HEADER_TO_CONNECTOR));
    header.add(resourceString(Constants.RES_TABLE_HEADER_ENABLED));

    List<String> ids = new LinkedList<String>();
    List<String> names = new LinkedList<String>();
    List<String> fromConnectors = new LinkedList<String>();
    List<String> toConnectors = new LinkedList<String>();
    List<String> availabilities = new LinkedList<String>();

    for(MJob job : jobs) {
      ids.add(String.valueOf(job.getPersistenceId()));
      names.add(job.getName());
      // From link and connnector
      fromConnectors.add(job.getFromLinkName() + " (" + job.getFromConnectorName() + ")");
      toConnectors.add(job.getToLinkName() + " (" + job.getToConnectorName() + ")");

      availabilities.add(String.valueOf(job.getEnabled()));
    }

    TableDisplayer.display(header, ids, names, fromConnectors, toConnectors, availabilities);
  }

  private void showJobs(String jArg) {
    List<MJob> jobs;
    if (jArg == null) {
      jobs = client.getJobs();
    } else {
      jobs = client.getJobsByConnector(jArg);
    }
    printlnResource(Constants.RES_SHOW_PROMPT_JOBS_TO_SHOW, jobs.size());

    for (MJob job : jobs) {
      displayJob(job);
    }
  }

  private void showJob(String jobArg) {
    MJob job = client.getJob(jobArg);
    printlnResource(Constants.RES_SHOW_PROMPT_JOBS_TO_SHOW, 1);

    displayJob(job);
  }

  private void displayJob(MJob job) {
    DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);

    printlnResource(
      Constants.RES_SHOW_PROMPT_JOB_INFO,
      job.getName(),
      job.getEnabled(),
      job.getCreationUser(),
      formatter.format(job.getCreationDate()),
      job.getLastUpdateUser(),
      formatter.format(job.getLastUpdateDate())
    );

    displayConfig(job.getDriverConfig().getConfigs(),
            client.getDriverConfigBundle());
    printlnResource(Constants.RES_SHOW_PROMPT_JOB_FROM_LID_INFO,
        job.getFromLinkName());
    displayConfig(job.getFromJobConfig().getConfigs(),
                 client.getConnectorConfigBundle(job.getFromConnectorName()));
    printlnResource(Constants.RES_SHOW_PROMPT_JOB_TO_LID_INFO,
            job.getToLinkName());
    displayConfig(job.getToJobConfig().getConfigs(),
                 client.getConnectorConfigBundle(job.getToConnectorName()));
  }
}
