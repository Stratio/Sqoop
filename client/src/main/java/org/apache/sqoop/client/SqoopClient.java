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
package org.apache.sqoop.client;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.client.request.SqoopResourceRequests;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.*;
import org.apache.sqoop.model.*;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.Status;

/**
 * Sqoop client API.
 *
 * High level Sqoop client API to communicate with Sqoop server. Current
 * implementation is not thread safe.
 *
 * SqoopClient is keeping cache of objects that are unlikely to be changed
 * (Resources, Connector structures). Volatile structures (Links, Jobs)
 * are not cached.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SqoopClient {

  /**
   * Underlying request object to fetch data from Sqoop server.
   */
  private SqoopResourceRequests resourceRequests;

  /**
   * True if user retrieved all connectors at once.
   */
  private boolean isAllConnectors;
  /**
   * All cached connectors.
   */
  private Map<String, MConnector> connectors;
  /**
   * All cached config params for every registered connector in the sqoop system.
   */
  private Map<String, ResourceBundle> connectorConfigBundles;

  /**
   * Cached driver.
   */
  private MDriver mDriver;
  /**
   * Cached driverConfig bundle.
   */
  private ResourceBundle driverConfigBundle;

  /**
   * Status flags used when updating the submission callback status
   */
  //TODO(https://issues.apache.org/jira/browse/SQOOP-1652): Why do wee need a duplicate status enum in client when shell is using the server status?
  // NOTE: the getStatus method is on the job resource and this needs to be revisited
  private enum SubmissionStatus {
    SUBMITTED,
    UPDATED,
    FINISHED
  }

  public SqoopClient(String serverUrl) {
    resourceRequests = new SqoopResourceRequests();
    setServerUrl(serverUrl);
  }

  /**
   * Set new server URL.
   *
   * Setting new URL will also clear all caches used by the client.
   *
   * @param serverUrl Server URL
   */
  public void setServerUrl(String serverUrl) {
    resourceRequests.setServerUrl(serverUrl);
    clearCache();
  }

  public String getServerUrl() {
    return resourceRequests.getServerUrl();
  }

  /**
   * Set arbitrary request object.
   *
   * @param requests SqoopRequests object
   */
  public void setSqoopRequests(SqoopResourceRequests requests) {
    this.resourceRequests = requests;
    clearCache();
  }

  /**
   * Clear internal cache.
   */
  public void clearCache() {
    connectorConfigBundles = new HashMap<String, ResourceBundle>();
    driverConfigBundle = null;
    connectors = new HashMap<String, MConnector>();
    mDriver = null;
    isAllConnectors = false;
  }

  /**
   * Get connector with given id.
   * TODO: This method should be removed when MJob link with Connector by name.
   *
   * @param cid Connector id.
   * @return
   */
  public MConnector getConnector(long cid) {
    return retrieveConnector(Long.toString(cid));
  }

  /**
   * Return connector with given name.
   *
   * @param connectorName Connector name
   * @return Connector model or NULL if the connector do not exists.
   */
  public MConnector getConnector(String connectorName) {
    // Firstly try if we have this connector already in cache
    MConnector connector = getConnectorFromCache(connectorName);
    if(connector != null) return connector;

    // If the connector wasn't in cache and we have all connectors,
    // it simply do not exists.
    if(isAllConnectors) return null;

    // Retrieve all connectors from server
    getConnectors();
    return getConnectorFromCache(connectorName);
  }

  /**
   * Iterate over cached connectors and return connector of given name.
   * This method will not contact server in case that the connector is
   * not found in the cache.
   *
   * @param connectorName Connector name
   * @return
   */
  private MConnector getConnectorFromCache(String connectorName) {
    for(MConnector connector : connectors.values()) {
      if(connector.getUniqueName().equals(connectorName)) {
        return connector;
      }
    }

    return null;
  }

  /**
   * Retrieve connector structure from server and cache it.
   * TODO: The method support both connector name/id, this should support name only when MJob link with Connector by name.
   *
   * @param connectorIdentify Connector name/id
   */
  private MConnector retrieveConnector(String connectorIdentify) {
    ConnectorBean request = resourceRequests.readConnector(connectorIdentify);
    MConnector mConnector = request.getConnectors().get(0);
    String connectorName = mConnector.getUniqueName();
    connectors.put(connectorName, mConnector);
    connectorConfigBundles.put(connectorName, request.getResourceBundles().get(connectorName));
    return mConnector;
  }

  /**
   * Get list of all connectors.
   *
   * @return
   */
  public Collection<MConnector> getConnectors() {
    if(isAllConnectors) {
      return connectors.values();
    }

    ConnectorBean bean = resourceRequests.readConnector(null);
    isAllConnectors = true;
    for(MConnector connector : bean.getConnectors()) {
      connectors.put(connector.getUniqueName(), connector);
    }
    connectorConfigBundles = bean.getResourceBundles();

    return connectors.values();
  }

  /**
   * Get resource bundle for given connector.
   *
   * @param connectorId Connector id.
   * @return
   */
  public ResourceBundle getConnectorConfigBundle(String connectorName) {
    if(connectorConfigBundles.containsKey(connectorName)) {
      return connectorConfigBundles.get(connectorName);
    }
    retrieveConnector(connectorName);
    return connectorConfigBundles.get(connectorName);
  }

  /**
   * Return driver config.
   *
   * @return
   */
  public MDriverConfig getDriverConfig() {
    if (mDriver != null) {
      return mDriver.clone(false).getDriverConfig();
    }
    retrieveAndCacheDriver();
    return mDriver.clone(false).getDriverConfig();
  }
  
  /**
   * Return driver.
   *
   * @return
   */
  public MDriver getDriver() {
    if (mDriver != null) {
      return mDriver.clone(false);
    }
    retrieveAndCacheDriver();
    return mDriver.clone(false);
 
  }

  /**
   * Retrieve driverConfig and cache it.
   */
  private void retrieveAndCacheDriver() {
    DriverBean driverBean =  resourceRequests.readDriver();
    mDriver = driverBean.getDriver();
    driverConfigBundle = driverBean.getDriverConfigResourceBundle();
  }

  /**
   * Return driverConfig bundle.
   *xx
   * @return
   */
  public ResourceBundle getDriverConfigBundle() {
    if(driverConfigBundle != null) {
      return driverConfigBundle;
    }
    retrieveAndCacheDriver();
    return driverConfigBundle;
  }

  /**
   * Create new link object for given connector name
   *
   * @param connectorName Connector name
   * @return
   */
  public MLink createLink(String connectorName) {
    MConnector connector = getConnector(connectorName);
    if (connector == null) {
      throw new SqoopException(ClientError.CLIENT_0003, connectorName);
    }
    return new MLink(connectorName, getConnector(connectorName).getLinkConfig());
  }


  /**
   * Retrieve link for given id.
   *
   * @param linkId Link id
   * @return
   */
  public MLink getLink(long linkId) {
    //Cast long to string and pass (retained to prevent other functionality from breaking)
    return resourceRequests.readLink(String.valueOf(linkId)).getLinks().get(0);
  }

  /**
   * Retrieve link for given name.
   *
   * @param linkName Link name
   * @return
   */
  public MLink getLink(String linkName) {
    return resourceRequests.readLink(linkName).getLinks().get(0);
  }

  /**
   * Retrieve list of all links.
   *
   * @return
   */
  public List<MLink> getLinks() {
    return resourceRequests.readLink(null).getLinks();
  }

  /**
   * Create the link and save to the repository
   *
   * @param link link that should be created
   * @return
   */
  public Status saveLink(MLink link) {
    return applyLinkValidations(resourceRequests.saveLink(link), link);
  }

  /**
   * Update link on the server.
   *
   * @param link link that should be updated
   * @return
   */
  public Status updateLink(MLink link) {
    return applyLinkValidations(resourceRequests.updateLink(link), link);
  }

  /**
   * Enable/disable link with given name
   *
   * @param linkName link name
   * @param enabled Enable or disable
   */
  public void enableLink(String linkName, boolean enabled) {
    resourceRequests.enableLink(linkName, enabled);
  }

  /**
   * Enable/disable link with given id
   *
   * @param linkId link id
   * @param enabled Enable or disable
   */
  public void enableLink(long linkId, boolean enabled) {
    resourceRequests.enableLink(String.valueOf(linkId), enabled);
  }

  /**
   * Delete link with given name
   *
   * @param linkName link name
   */
  public void deleteLink(String linkName) {
    resourceRequests.deleteLink(linkName);
  }

  /**
   * Delete link with given id.
   *
   * @param linkId link id
   */
  public void deleteLink(long linkId) {
    resourceRequests.deleteLink(String.valueOf(linkId));
  }

  /**
   * Create new job the for given links.
   *
   * @param fromLinkName From link name
   * @param toLinkName To link name
   * @return
   */
  public MJob createJob(String fromLinkName, String toLinkName) {
    MLink fromLink = getLink(fromLinkName);
    MLink toLink = getLink(toLinkName);
    MConnector connectorForFromLink = getConnector(fromLink.getConnectorName());
    MConnector connectorForToLink = getConnector(toLink.getConnectorName());

    return new MJob(
      connectorForFromLink.getUniqueName(),
      connectorForToLink.getUniqueName(),
      fromLinkName,
      toLinkName,
      connectorForFromLink.getFromConfig().clone(false),
      connectorForToLink.getToConfig().clone(false),
      getDriverConfig()
    );
  }

  /**
   * Retrieve job for given id.
   *
   * @param jobId Job id
   * @return
   */
  public MJob getJob(long jobId) {
    //Cast long to string and pass (retained to prevent other functionality from breaking)
    return resourceRequests.readJob(String.valueOf(jobId)).getJobs().get(0);
  }

  /**
   * Retrieve job for given name.
   *
   * @param jobName Job name
   * @return
   */
  public MJob getJob(String jobName) {
    return resourceRequests.readJob(jobName).getJobs().get(0);
  }

  /**
   * Retrieve list of all jobs.
   *
   * @return
   */
  public List<MJob> getJobs() {
    return resourceRequests.readJob(null).getJobs();
  }

  /**
   * Retrieve list of all jobs by connector id
   *
   * @return
   */
  public List<MJob> getJobsByConnector(long cId) {
    return resourceRequests.readJobsByConnector(String.valueOf(cId)).getJobs();
  }

  /**
   * Retrieve list of all jobs by connector name
   *
   * @return
   */
  public List<MJob> getJobsByConnector(String cName) {
    return resourceRequests.readJobsByConnector(cName).getJobs();
  }

  /**
   * Create job on server and save to the repository
   *
   * @param job Job that should be created
   * @return
   */
  public Status saveJob(MJob job) {
    return applyJobValidations(resourceRequests.saveJob(job), job);
  }

  /**
   * Update job on server.
   * @param job Job that should be updated
   * @return
   */
  public Status updateJob(MJob job) {
    return applyJobValidations(resourceRequests.updateJob(job), job);
  }

  /**
   * Enable/disable job with given name
   *
   * @param jName Job that is going to be enabled/disabled
   * @param enabled Enable or disable
   */
  public void enableJob(String jName, boolean enabled) {
    resourceRequests.enableJob(jName, enabled);
  }

  /**
   * Enable/disable job with given id
   *
   * @param jId Job that is going to be enabled/disabled
   * @param enabled Enable or disable
   */
  public void enableJob(long jId, boolean enabled) {
    resourceRequests.enableJob(String.valueOf(jId), enabled);
  }

  /**
   * Delete job with given name.
   *
   * @param jobName Job name
   */
  public void deleteJob(String jobName) {
    resourceRequests.deleteJob(jobName);
  }

  /**
   * Delete job with given id
   *
   * @param jobId Job id
   */
  public void deleteJob(long jobId) {
    resourceRequests.deleteJob(String.valueOf(jobId));
  }

  public void deleteAllLinks(){
    for (MJob job : getJobs()) {
      deleteJob(job.getName());
    }
  }

  public void deleteAllJobs(){
    for (MLink link : getLinks()) {
      deleteLink(link.getName());
    }
  }

  public void deleteAllLinksAndJobs(){
    deleteAllLinks();
    deleteAllJobs();
  }

  /**
   * Start job with given name.
   *
   * @param jobName Job name
   * @return
   */
  public MSubmission startJob(String jobName) {
    return resourceRequests.startJob(jobName).getSubmissions().get(0);
  }

  /**
   * Start job with given id.
   *
   * @param jobId Job id
   * @return
   */
  public MSubmission startJob(long jobId) {
    return resourceRequests.startJob(String.valueOf(jobId)).getSubmissions().get(0);
  }

  /**
   * Method used for synchronous job submission.
   * Pass null to callback parameter if submission status is not required and after completion
   * job execution returns MSubmission which contains final status of submission.
   * @param jobName - Job name
   * @param callback - User may set null if submission status is not required, else callback methods invoked
   * @param pollTime - Server poll time
   * @return MSubmission - Final status of job submission
   * @throws InterruptedException
   */
  public MSubmission startJob(String jobName, SubmissionCallback callback, long pollTime)
      throws InterruptedException {
    if(pollTime <= 0) {
      throw new SqoopException(ClientError.CLIENT_0002);
    }
    //TODO(https://issues.apache.org/jira/browse/SQOOP-1652): address the submit/start/first terminology difference
    // What does first even mean in s distributed client/server model?
    boolean first = true;
    MSubmission submission = resourceRequests.startJob(jobName).getSubmissions().get(0);
    // what happens when the server fails, do we just say finished?
    while(submission.getStatus().isRunning()) {
      if(first) {
        invokeSubmissionCallback(callback, submission, SubmissionStatus.SUBMITTED);
        first = false;
      } else {
        invokeSubmissionCallback(callback, submission, SubmissionStatus.UPDATED);
      }
      Thread.sleep(pollTime);

      //Works with both name as well as id (in string form) as argument
      submission = getJobStatus(jobName);
    }
    invokeSubmissionCallback(callback, submission, SubmissionStatus.FINISHED);
    return submission;
  }

    public MSubmission startJob(long jobId, SubmissionCallback callback, long pollTime)
            throws InterruptedException {
      return startJob(String.valueOf(jobId), callback, pollTime);
    }

  /**
   * Invokes the callback's methods with MSubmission object
   * based on SubmissionStatus. If callback is null, no operation performed.
   * @param callback
   * @param submission
   * @param status
   */
  private void invokeSubmissionCallback(SubmissionCallback callback, MSubmission submission,
      SubmissionStatus status) {
    if (callback == null) {
      return;
    }
    switch (status) {
    case SUBMITTED:
      callback.submitted(submission);
      break;
    case UPDATED:
      callback.updated(submission);
      break;
    case FINISHED:
      callback.finished(submission);
    default:
      break;
    }
  }

  /**
   * stop job with given name.
   *
   * @param jName Job name
   * @return
   */
  public MSubmission stopJob(String jName) {
    return resourceRequests.stopJob(jName).getSubmissions().get(0);
  }

  /**
   * stop job with given id.
   *
   * @param jId Job id
   * @return
   */
  public MSubmission stopJob(long jId) {
    return resourceRequests.stopJob(String.valueOf(jId)).getSubmissions().get(0);
  }

  /**
   * Get status for given job name.
   *
   * @param jName Job name
   * @return
   */
  public MSubmission getJobStatus(String jName) {
    return resourceRequests.getJobStatus(jName).getSubmissions().get(0);
  }

  /**
   * Get status for given job id.
   *
   * @param jid Job id
   * @return
   */
  public MSubmission getJobStatus(long jid) {
    return resourceRequests.getJobStatus(String.valueOf(jid)).getSubmissions().get(0);
  }

  /**
   * Retrieve list of all submissions.
   *
   * @return
   */
  public List<MSubmission> getSubmissions() {
    return resourceRequests.readSubmission(null).getSubmissions();
  }

  /**
   * Retrieve list of submissions for given jobId.
   *
   * @param jobId Job id
   * @return
   */
  public List<MSubmission> getSubmissionsForJob(long jobId) {
    return resourceRequests.readSubmission(String.valueOf(jobId)).getSubmissions();
  }

  /**
   * Retrieve list of submissions for given job name.
   *
   * @param jobName Job name
   * @return
   */
  public List<MSubmission> getSubmissionsForJob(String jobName) {
    return resourceRequests.readSubmission(jobName).getSubmissions();
  }

  /**
   * Retrieve list of all roles.
   *
   * @return
   */
  public List<MRole> getRoles() {
    return resourceRequests.readRoles().getRoles();
  }

  /**
   * Create a new role.
   *
   * @param role MRole
   * @return
   */
  public void createRole(MRole role) {
    resourceRequests.createRole(role);
  }

  /**
   * Drop a role.
   *
   * @param role MRole
   * @return
   */
  public void dropRole(MRole role) {
    resourceRequests.dropRole(role);
  }

  /**
   * Grant roles on principals.
   *
   * @param roles      MRole List
   * @param principals MPrincipal List
   * @return
   */
  public void grantRole(List<MRole> roles, List<MPrincipal> principals) {
    resourceRequests.grantRole(roles, principals);
  }

  /**
   * Revoke roles on principals.
   *
   * @param roles      MRole List
   * @param principals MPrincipal List
   * @return
   */
  public void revokeRole(List<MRole> roles, List<MPrincipal> principals) {
    resourceRequests.revokeRole(roles, principals);
  }

  /**
   * Get roles by principal.
   *
   * @param principal MPrincipal
   * @return
   */
  public List<MRole> getRolesByPrincipal(MPrincipal principal) {
    return resourceRequests.readRolesByPrincipal(principal).getRoles();
  }

  /**
   * Get principals by role.
   *
   * @param role MRole
   * @return
   */
  public List<MPrincipal> getPrincipalsByRole(MRole role) {
    return resourceRequests.readPrincipalsByRole(role).getPrincipals();
  }

  /**
   * Grant privileges on principals.
   *
   * @param principals MPrincipal List
   * @param privileges MPrivilege List
   * @return
   */
  public void grantPrivilege(List<MPrincipal> principals, List<MPrivilege> privileges) {
    resourceRequests.grantPrivilege(principals, privileges);
  }

  /**
   * Revoke privileges on principals and will revoke all privileges on principals
   * if privileges is null.
   *
   * @param principals MPrincipal List
   * @param privileges MPrivilege List
   * @return
   */
  public void revokePrivilege(List<MPrincipal> principals, List<MPrivilege> privileges) {
    resourceRequests.revokePrivilege(principals, privileges);
  }

  /**
   * Get privileges by principal.
   *
   * @param principal MPrincipal
   * @param resource MResource
   * @return
   */
  public List<MPrivilege> getPrivilegesByPrincipal(MPrincipal principal, MResource resource) {
    return resourceRequests.readPrivilegesByPrincipal(principal, resource).getPrivileges();
  }

  /**
   * Add delegation token into credentials of Hadoop security.
   *
   * @param renewer renewer string
   * @param credentials credentials of Hadoop security, which will be added delegation token
   * @return
   */
  public Token<?>[] addDelegationTokens(String renewer,
                                        Credentials credentials) throws IOException {
    return resourceRequests.addDelegationTokens(renewer, credentials);
  }

  public VersionBean readVersion() {
    return resourceRequests.readVersion();
  }

  private Status applyLinkValidations(ValidationResultBean bean, MLink link) {
    ConfigValidationResult linkConfig = bean.getValidationResults()[0];
    // Apply validation results
    ConfigUtils.applyValidation(link.getConnectorLinkConfig(), linkConfig);
    Long id = bean.getId();
    if (id != null) {
      link.setPersistenceId(id);
    }
    return Status.getWorstStatus(linkConfig.getStatus());
  }


  private Status applyJobValidations(ValidationResultBean bean, MJob job) {
    ConfigValidationResult fromConfig = bean.getValidationResults()[0];
    ConfigValidationResult toConfig = bean.getValidationResults()[1];
    ConfigValidationResult driver = bean.getValidationResults()[2];

    ConfigUtils.applyValidation(
        job.getFromJobConfig(),
        fromConfig);
    ConfigUtils.applyValidation(
        job.getToJobConfig(),
        toConfig);
    ConfigUtils.applyValidation(
      job.getDriverConfig(),
      driver
    );

    Long id = bean.getId();
    if(id != null) {
      job.setPersistenceId(id);
    }

    return Status.getWorstStatus(fromConfig.getStatus(), toConfig.getStatus(), driver.getStatus());
  }
}
