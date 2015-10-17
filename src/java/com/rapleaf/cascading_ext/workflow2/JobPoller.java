package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_tools.jobs.ActionOperation;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowStatePersistence;

class JobPoller extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(JobPoller.class);
  private static final long THIRTY_SECONDS = 30000;

  private boolean shouldShutdown = false;

  private final String checkpoint;
  private final List<ActionOperation> actionList;
  private final WorkflowStatePersistence persistence;

  public JobPoller(String checkpoint,
                   List<ActionOperation> actionList,
                   WorkflowStatePersistence persistence) {
    this.checkpoint = checkpoint;
    this.actionList = actionList;
    this.persistence = persistence;

    setDaemon(true);
  }

  public void shutdown() {
    this.shouldShutdown = true;
    interrupt();
  }

  @Override
  public void run() {
    while (!shouldShutdown) {
      try {
        updateRunningJobs();
        Thread.sleep(THIRTY_SECONDS);
      } catch (InterruptedException e) {
        //  expected
      }
    }
  }

  public void updateRunningJobs() {
    for (ActionOperation operation : actionList) {
      for (RunningJob job : operation.listJobs()) {
        try {
          if (job.getJobState() == JobStatus.State.RUNNING.getValue()) {
            persistence.markStepRunningJob(
                checkpoint,
                job.getID().toString(),
                job.getJobName(),
                job.getTrackingURL()
            );
          }
        } catch (NullPointerException | IOException e) {
          LOG.error("Cannot mark job as running", e);
        }
      }

    }
  }

}
