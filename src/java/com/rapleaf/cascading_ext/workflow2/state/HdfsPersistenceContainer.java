package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.rapleaf.cascading_ext.workflow2.WorkflowUtil;
import com.rapleaf.db_schemas.rldb.workflow.DataStoreInfo;
import com.rapleaf.db_schemas.rldb.workflow.MapReduceJob;
import com.rapleaf.db_schemas.rldb.workflow.StepState;
import com.rapleaf.db_schemas.rldb.workflow.StepStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowStatePersistence;

public class HdfsPersistenceContainer implements WorkflowStatePersistence {
  private static final Logger LOG = Logger.getLogger(HdfsPersistenceContainer.class);

  private final String checkpointDir;
  private final boolean deleteCheckpointsOnSuccess;
  private final FileSystem fs;

  private final Map<String, StepState> statuses;
  private final List<DataStoreInfo> datastores;
  private String shutdownReason;

  private final String id;
  private final String name;
  private String priority;
  private String pool;
  private final String host;
  private final String username;

  public HdfsPersistenceContainer(String checkpointDir,
                                  boolean deleteOnSuccess,
                                  String id,
                                  String name,
                                  String priority,
                                  String pool,
                                  String host,
                                  String username,
                                  Map<String, StepState> statuses,
                                  List<DataStoreInfo> datastores) {

    this.checkpointDir = checkpointDir;
    this.deleteCheckpointsOnSuccess = deleteOnSuccess;
    this.fs = FileSystemHelper.getFS();

    this.id = id;
    this.name = name;
    this.priority = priority;
    this.pool = pool;
    this.host = host;
    this.username = username;
    this.statuses = statuses;
    this.datastores = datastores;

  }


  @Override
  public void markShutdownRequested(String reason) {
    shutdownReason = WorkflowUtil.getShutdownReason(reason);
  }

  @Override
  public void markWorkflowStopped() {

    if (allStepsSucceeded() && shutdownReason == null) {
      try {
        if (deleteCheckpointsOnSuccess) {
          LOG.debug("Deleting checkpoint dir " + checkpointDir);
          TrashHelper.deleteUsingTrashIfEnabled(fs, new Path(checkpointDir));
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  private boolean allStepsSucceeded() {

    for (Map.Entry<String, StepState> stepStatuses : statuses.entrySet()) {
      if (!StepStatus.NON_BLOCKING.contains(stepStatuses.getValue().getStatus())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public StepState getState(String stepToken) {

    if (!statuses.containsKey(stepToken)) {
      throw new RuntimeException("Unknown step " + stepToken + "!");
    }

    return statuses.get(stepToken);
  }

  @Override
  public Map<String, StepState> getStepStatuses() {
    return statuses;
  }

  @Override
  public List<DataStoreInfo> getDatastores() {
    return datastores;
  }

  @Override
  public String getShutdownRequest() {
    return shutdownReason;
  }

  @Override
  public String getPriority() {
    return priority;
  }

  @Override
  public String getPool() {
    return pool;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public String getUsername() {
    return username;
  }

  @Override
  public void markStepRunning(String stepToken) throws IOException {
    getState(stepToken)
        .setStatus(StepStatus.RUNNING)
        .setStartTimestamp(System.currentTimeMillis());
  }

  @Override
  public void markStepFailed(String stepToken, Throwable e) throws IOException {

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);

    getState(stepToken)
        .setFailureMessage(e.getMessage())
        .setFailureTrace(sw.toString())
        .setStatus(StepStatus.FAILED)
        .setEndTimestamp(System.currentTimeMillis());

  }

  @Override
  public void markStepSkipped(String stepToken) throws IOException {
    getState(stepToken)
        .setStatus(StepStatus.SKIPPED);
  }

  @Override
  public void markStepCompleted(String stepToken) throws IOException {
    LOG.info("Writing out checkpoint token for " + stepToken);
    String tokenPath = checkpointDir + "/" + stepToken;
    if (!fs.createNewFile(new Path(tokenPath))) {
      throw new IOException("Couldn't create checkpoint file " + tokenPath);
    }
    LOG.debug("Done writing checkpoint token for " + stepToken);

    getState(stepToken)
        .setStatus(StepStatus.COMPLETED)
        .setEndTimestamp(System.currentTimeMillis());
  }

  @Override
  public void markStepStatusMessage(String stepToken, String newMessage) {
    getState(stepToken)
        .setStatusMessage(newMessage);
  }

  @Override
  public void markStepRunningJob(String stepToken, String jobId, String jobName, String trackingURL) {

    Set<String> knownJobs = Sets.newHashSet();
    StepState stepState = getState(stepToken);

    for (String existingId : stepState.getMrJobsByID().keySet()) {
      knownJobs.add(existingId);
    }

    if (!knownJobs.contains(jobId)) {
      stepState.addMrjob(new MapReduceJob(jobId, jobName, trackingURL));
    }

  }

  @Override
  public void markPool(String pool) {
    this.pool = pool;
  }

  @Override
  public void markPriority(String priority) {
    this.priority = priority;
  }



}
