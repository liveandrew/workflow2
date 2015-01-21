package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowUtil;
import com.rapleaf.support.Rap;

public class HdfsCheckpointPersistence implements WorkflowStatePersistence {
  private static final Logger LOG = Logger.getLogger(HdfsCheckpointPersistence.class);

  private final String checkpointDir;
  private final boolean deleteCheckpointsOnSuccess;
  private final FileSystem fs;

  private final Map<String, StepState> statuses = Maps.newHashMap();
  private final List<DataStoreInfo> datastores = Lists.newArrayList();
  private String shutdownReason;

  private String id;
  private String description;
  private String priority;
  private String pool;
  private String host;
  private String username;

  public HdfsCheckpointPersistence(String checkpointDir) {
    this(checkpointDir, true);
  }

  public HdfsCheckpointPersistence(String checkpointDir, boolean deleteOnSuccess) {
    this.checkpointDir = checkpointDir;
    this.deleteCheckpointsOnSuccess = deleteOnSuccess;
    this.fs = FileSystemHelper.getFS();
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
  public String getDescription() {
    return description;
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
  public void markStepRunningJob(String stepToken, RunningJob job) {

    Set<String> knownJobs = Sets.newHashSet();
    StepState stepState = getState(stepToken);

    for (String jobId : stepState.getMrJobsByID().keySet()) {
      knownJobs.add(jobId);
    }

    if (!knownJobs.contains(job.getID().toString())) {
      stepState.addMrjob(new MapReduceJob(job.getID().toString(), job.getJobName(), job.getTrackingURL()));
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

  @Override
  public void prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                      String description,
                      String host,
                      String username,
                      String pool,
                      String priority) {

    this.id = Hex.encodeHexString(Rap.uuidToBytes(UUID.randomUUID()));
    this.description = description;
    this.username = username;
    this.host = host;
    this.pool = pool;
    this.priority = priority;

    try {
      LOG.info("Creating checkpoint dir " + checkpointDir);
      fs.mkdirs(new Path(checkpointDir));

      Map<DataStore, DataStoreInfo> dataStoreToRep = Maps.newHashMap();

      for (Step val : flatSteps.vertexSet()) {
        Action action = val.getAction();

        Set<String> dependencies = Sets.newHashSet();
        for (DefaultEdge edge : flatSteps.outgoingEdgesOf(val)) {
          dependencies.add(flatSteps.getEdgeTarget(edge).getCheckpointToken());
        }

        Multimap<Action.DSAction, DataStoreInfo> stepDsInfo = HashMultimap.create();

        for (Map.Entry<Action.DSAction, DataStore> entry : val.getAction().getAllDatastores().entries()) {
          DataStore dataStore = entry.getValue();

          if (!dataStoreToRep.containsKey(dataStore)) {

            DataStoreInfo info = new DataStoreInfo(
                dataStore.getName(),
                dataStore.getClass().getName(),
                dataStore.getPath(),
                dataStoreToRep.size()
            );

            dataStoreToRep.put(dataStore, info);
            datastores.add(info);

          }

          stepDsInfo.put(entry.getKey(), dataStoreToRep.get(dataStore));

        }

        statuses.put(val.getCheckpointToken(), new StepState(
            val.getCheckpointToken(),
            StepStatus.WAITING,
            action.getClass().getSimpleName(),
            dependencies,
            stepDsInfo
        ));

      }


      for (FileStatus status : FileSystemHelper.safeListStatus(fs, new Path(checkpointDir))) {
        String token = status.getPath().getName();
        if (statuses.containsKey(token)) {
          statuses.get(token).setStatus(StepStatus.SKIPPED);
        } else {
          LOG.info("Skipping obsolete token " + token);
        }
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
