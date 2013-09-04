package com.rapleaf.cascading_ext.workflow2.state;

import com.google.common.collect.Maps;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.workflow_service.generated.*;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.liveramp.cascading_ext.fs.TrashHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class HdfsCheckpointPersistence implements WorkflowStatePersistence {
  private static final Logger LOG = Logger.getLogger(HdfsCheckpointPersistence.class);

  private final String checkpointDir;
  private final boolean deleteCheckpointsOnSuccess;
  private final FileSystem fs;

  private final Map<String, StepExecuteStatus> statuses = Maps.newHashMap();
  private ExecuteStatus currentStatus;

  public HdfsCheckpointPersistence(String checkpointDir) {
    this(checkpointDir, true);
  }

  public HdfsCheckpointPersistence(String checkpointDir, boolean deleteOnSuccess) {
    this.checkpointDir = checkpointDir;
    this.deleteCheckpointsOnSuccess = deleteOnSuccess;
    this.fs = FileSystemHelper.getFS();

    try {
      for (FileStatus status : FileSystemHelper.safeListStatus(fs, new Path(checkpointDir))) {
        statuses.put(status.getPath().getName(), StepExecuteStatus.skipped(new StepSkippedMeta()));
      }
    } catch (Exception e) {
      throw new RuntimeException("Error reading from checkpoint directory!", e);
    }

    this.currentStatus = ExecuteStatus.active(new ActiveState(ActiveStatus.running(new RunningMeta())));
  }

  @Override
  public Map<String, StepExecuteStatus> getAllStepStatuses() {
    return statuses;
  }

  @Override
  public StepExecuteStatus getStatus(Step step) {
    String checkpoint = step.getCheckpointToken();

    //  if we know the status, return it
    if (!statuses.containsKey(checkpoint)) {
      //  we haven't seen it before, it's waiting
      statuses.put(checkpoint, StepExecuteStatus.waiting(new StepWaitingMeta()));
    }

    return statuses.get(checkpoint);
  }

  @Override
  public ExecuteStatus getFlowStatus() {
    return currentStatus;
  }

  @Override
  public void updateStatus(Step step, StepExecuteStatus status) throws IOException {
    LOG.info("Noting new status for step " + step.getCheckpointToken() + ": " + status);

    if (status.isSetCompleted()) {
      LOG.info("Writing out checkpoint token for " + step.getCheckpointToken());
      String tokenPath = checkpointDir + "/" + step.getCheckpointToken();
      if (!fs.createNewFile(new Path(tokenPath))) {
        throw new IOException("Couldn't create checkpoint file " + tokenPath);
      }
      LOG.debug("Done writing checkpoint token for " + step.getCheckpointToken());
    } else if (status.isSetFailed()) {
      currentStatus = ExecuteStatus.active(new ActiveState(ActiveStatus.failPending(new FailMeta())));
    }

    statuses.put(step.getCheckpointToken(), status);
  }

  @Override
  public void prepare(WorkflowDefinition def) throws IOException {
    LOG.info("Creating checkpoint dir " + checkpointDir);
    fs.mkdirs(new Path(checkpointDir));
  }

  @Override
  public void setStatus(ExecuteStatus status) {
    currentStatus = status;
    if (status.isSetComplete()) {
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
}
