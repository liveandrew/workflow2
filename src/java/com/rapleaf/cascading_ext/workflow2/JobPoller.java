package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapred.RunningJob;

import com.rapleaf.cascading_ext.workflow2.state.WorkflowStatePersistence;

class JobPoller extends Thread {
  private static final long TEN_SECONDS = 10000;

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
        Thread.sleep(TEN_SECONDS);
      } catch (InterruptedException e) {
        //  expected
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void updateRunningJobs() throws IOException {
    for (ActionOperation operation : actionList) {
      try {
        for (RunningJob job : operation.listJobs()) {
          persistence.markStepRunningJob(checkpoint, job);
        }
      } catch (NullPointerException e) {
        //  no op
      }
    }
  }
}
