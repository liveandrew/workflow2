package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.workflow_service.generated.StepCompletedMeta;
import com.liveramp.workflow_service.generated.StepExecuteStatus;
import com.liveramp.workflow_service.generated.StepFailedMeta;
import com.liveramp.workflow_service.generated.StepRunningMeta;
import com.liveramp.workflow_service.generated.StepSkippedMeta;
import com.liveramp.workflow_service.generated.StepWaitingMeta;
import com.liveramp.workflow_service.generated.WorkflowDefinition;
import com.liveramp.workflow_service.generated.WorkflowException;

public class HdfsCheckpointPersistence implements WorkflowStatePersistence {
  private static final Logger LOG = Logger.getLogger(HdfsCheckpointPersistence.class);

  private final String checkpointDir;
  private final boolean deleteCheckpointsOnSuccess;
  private final FileSystem fs;

  private final Map<String, StepExecuteStatus> statuses = Maps.newHashMap();
  private String shutdownReason;

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
    shutdownReason = reason;
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

    for (Map.Entry<String, StepExecuteStatus> stepStatuses : statuses.entrySet()) {
      if (!NON_BLOCKING.contains(stepStatuses.getValue().getSetField())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public StepExecuteStatus getStatus(String stepToken) {

    if (!statuses.containsKey(stepToken)) {
      throw new RuntimeException("Unknown step " + stepToken + "!");
    }

    return statuses.get(stepToken);
  }

  @Override
  public WorkflowState getFlowStatus() {
    return new WorkflowState(statuses, shutdownReason);
  }

  @Override
  public void markStepRunning(String stepToken) throws IOException {
    updateStatus(stepToken, StepExecuteStatus.running(new StepRunningMeta()));
  }

  @Override
  public void markStepFailed(String stepToken, Throwable e) throws IOException {

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);

    updateStatus(stepToken, StepExecuteStatus.failed(new StepFailedMeta(new WorkflowException(e.getMessage(), sw.toString()))));
  }

  @Override
  public void markStepSkipped(String stepToken) throws IOException {
    updateStatus(stepToken, StepExecuteStatus.skipped(new StepSkippedMeta()));
  }

  @Override
  public void markStepCompleted(String stepToken) throws IOException {
    LOG.info("Writing out checkpoint token for " + stepToken);
    String tokenPath = checkpointDir + "/" + stepToken;
    if (!fs.createNewFile(new Path(tokenPath))) {
      throw new IOException("Couldn't create checkpoint file " + tokenPath);
    }
    LOG.debug("Done writing checkpoint token for " + stepToken);

    updateStatus(stepToken, StepExecuteStatus.completed(new StepCompletedMeta()));
  }

  private void updateStatus(String stepToken, StepExecuteStatus status) throws IOException {

    if(!statuses.containsKey(stepToken)){
      throw new RuntimeException(stepToken+"!");
    }

    LOG.info("Noting new status for step " + stepToken + ": " + status);
    statuses.put(stepToken, status);
  }

  @Override
  public void prepare(WorkflowDefinition def) {

    try {
      LOG.info("Creating checkpoint dir " + checkpointDir);
      fs.mkdirs(new Path(checkpointDir));

      for (FileStatus status : FileSystemHelper.safeListStatus(fs, new Path(checkpointDir))) {
        statuses.put(status.getPath().getName(), StepExecuteStatus.skipped(new StepSkippedMeta()));
      }

      for (String val : def.get_steps().keySet()) {
        //  if we know the status, return it
        if (!statuses.containsKey(val)) {
          //  we haven't seen it, it's waiting
          statuses.put(val, StepExecuteStatus.waiting(new StepWaitingMeta()));
        }
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
