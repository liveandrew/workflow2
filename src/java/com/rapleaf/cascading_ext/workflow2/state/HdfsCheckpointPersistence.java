package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.resource.CheckpointUtil;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow.types.StepStatus;
import com.liveramp.workflow_state.IStep;
import com.liveramp.workflow_state.StepState;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.support.Rap;

public class HdfsCheckpointPersistence extends WorkflowPersistenceFactory<HdfsInitializedPersistence, WorkflowOptions> {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsPersistenceContainer.class);

  private final String checkpointDir;
  private final boolean deleteOnSuccess;

  public HdfsCheckpointPersistence(String checkpointDir) {
    this(checkpointDir, true);
  }

  public HdfsCheckpointPersistence(String checkpointDir, boolean deleteOnSuccess) {
    this.checkpointDir = checkpointDir;
    this.deleteOnSuccess = deleteOnSuccess;
  }

  @Override
  public HdfsInitializedPersistence initializeInternal(String name,
                                                       String scopeId,
                                                       String description,
                                                       AppType appType,
                                                       String host,
                                                       String username,
                                                       String pool,
                                                       String priority,
                                                       String launchDir,
                                                       String launchJar,
                                                       Set<WorkflowRunnerNotification> configuredNotifications,
                                                       AlertsHandler providedHandler,
                                                       String remote,
                                                       String implementationBuild) throws IOException {

    FileSystem fs = FileSystemHelper.getFS();

    Path checkpointDirPath = new Path(checkpointDir);
    LOG.info("Creating checkpoint dir " + checkpointDir);
    fs.mkdirs(checkpointDirPath);

    long currentExecution = getAttemptExecutionId(fs, checkpointDirPath);
    LOG.info("Writing execution ID to state:  "+currentExecution);

    CheckpointUtil.writeExecutionId(currentExecution, fs, checkpointDirPath);

    return new HdfsInitializedPersistence(currentExecution, name, priority, pool, host, username, providedHandler, configuredNotifications, fs);
  }

  private long getAttemptExecutionId(FileSystem fs, Path checkpointDirPath) throws IOException {

    long latest = CheckpointUtil.getLatestExecutionId(fs, checkpointDirPath);

    //  we are resuming
    if (CheckpointUtil.existCheckpoints(checkpointDirPath)) {
      LOG.info("Resuming execution, using ID "+latest);
      return latest;
    }
    //  new execution
    else {

      long next = latest + 1;
      LOG.info("New execution, using ID "+next);

      return next;
    }

  }

  @Override
  public WorkflowStatePersistence prepare(HdfsInitializedPersistence persistence,
                                          DirectedGraph<IStep, DefaultEdge> flatSteps) {

    FileSystem fs = persistence.getFs();

    Map<String, StepState> statuses = Maps.newHashMap();

    try {

      for (IStep val : flatSteps.vertexSet()) {

        Set<String> dependencies = Sets.newHashSet();
        for (DefaultEdge edge : flatSteps.outgoingEdgesOf(val)) {
          dependencies.add(flatSteps.getEdgeTarget(edge).getCheckpointToken());
        }

        statuses.put(val.getCheckpointToken(), new StepState(
            val.getCheckpointToken(),
            StepStatus.WAITING,
            val.getActionClass(),
            dependencies,
            val.getDataStores()
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

      return new HdfsPersistenceContainer(
          checkpointDir,
          deleteOnSuccess,
          Hex.encodeHexString(Rap.uuidToBytes(UUID.randomUUID())),
          statuses,
          persistence
      );

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
