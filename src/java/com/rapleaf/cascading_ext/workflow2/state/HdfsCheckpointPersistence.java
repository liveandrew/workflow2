package com.rapleaf.cascading_ext.workflow2.state;

import com.google.common.collect.Maps;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.workflow_service.generated.StepStatus;
import com.liveramp.workflow_service.generated.WorkflowDefinition;
import com.rapleaf.cascading_ext.workflow2.Step;
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

  private final Map<String, StepStatus> statuses = Maps.newHashMap();

  public HdfsCheckpointPersistence(String checkpointDir){
    this(checkpointDir, true);
  }

  public HdfsCheckpointPersistence(String checkpointDir, boolean deleteOnSuccess) {
    this.checkpointDir = checkpointDir;
    this.deleteCheckpointsOnSuccess = deleteOnSuccess;
    this.fs = FileSystemHelper.getFS();

    try{
      for(FileStatus status: FileSystemHelper.safeListStatus(fs, new Path(checkpointDir))){
        statuses.put(status.getPath().getName(), StepStatus.SKIPPED);
      }
    }catch(Exception e){
      throw new RuntimeException("Error reading from checkpoint directory!", e);
    }
  }

  @Override
  public StepStatus getStatus(Step step) {
    String checkpoint = step.getCheckpointToken();

    //  if we know the status, return it
    if(!statuses.containsKey(checkpoint)){
      //  we haven't seen it before, it's waiting
      statuses.put(checkpoint, StepStatus.WAITING);
    }

    return statuses.get(checkpoint);
  }

  @Override
  public void updateStatus(Step step, StepStatus status) throws IOException {
    LOG.info("Noting new status for step "+step.getCheckpointToken()+": "+status);

    if(status == StepStatus.COMPLETED){
      LOG.info("Writing out checkpoint token for " + step.getCheckpointToken());
      String tokenPath = checkpointDir + "/" + step.getCheckpointToken();
      if (!fs.createNewFile(new Path(tokenPath))) {
        throw new IOException("Couldn't create checkpoint file " + tokenPath);
      }
      LOG.debug("Done writing checkpoint token for " + step.getCheckpointToken());
    }

    statuses.put(step.getCheckpointToken(), status);
  }

  @Override
  public void prepare(WorkflowDefinition def) throws IOException {
    LOG.info("Creating checkpoint dir " + checkpointDir);
    fs.mkdirs(new Path(checkpointDir));
  }

  @Override
  public void complete() throws IOException {
    if (deleteCheckpointsOnSuccess) {
      LOG.debug("Deleting checkpoint dir " + checkpointDir);
      fs.delete(new Path(checkpointDir), true);
    }
  }
}
