package com.liveramp.workflow_state;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

import static com.liveramp.workflow_state.DbPersistence.HEARTBEAT_INTERVAL;

public class InitializedDbPersistence implements InitializedPersistence {

  private static final Logger LOG = LoggerFactory.getLogger(InitializedDbPersistence.class);

  private final IRlDb rldb;
  private final long workflowAttemptId;
  private final Thread heartbeatThread;
  private final AlertsHandler providedHandler;

  public InitializedDbPersistence(long workflowAttemptId, IRlDb rldb, boolean runMode, AlertsHandler providedHandler) {
    this.rldb = rldb;
    this.rldb.disableCaching();
    this.workflowAttemptId = workflowAttemptId;

    if (runMode) {
      this.heartbeatThread = new Thread(new Heartbeat());
      this.heartbeatThread.setDaemon(true);
      this.heartbeatThread.start();
      this.providedHandler = providedHandler;
    } else {
      this.heartbeatThread = null;
      this.providedHandler = null;
    }
  }

  @Override
  public synchronized long getExecutionId() throws IOException {
    return getExecution().getId();
  }

  @Override
  public synchronized long getAttemptId() throws IOException {
    return workflowAttemptId;
  }

  //  this method is carefully not synchronized, because we don't want a deadlock with the heartbeat waiting to heartbeat.
  @Override
  public void stop() throws IOException {

    synchronized (this) {
      LOG.info("Marking workflow stopped");

      WorkflowAttempt attempt = getAttempt()
          .setEndTime(System.currentTimeMillis());

      if (attempt.getStatus() == AttemptStatus.INITIALIZING.ordinal()) {
        LOG.info("Workflow initialized without executing, assuming failure");
        attempt.setStatus(AttemptStatus.FAILED.ordinal());
      }

      else {

        if (WorkflowQueries.workflowComplete(getExecution())) {
          LOG.info("Marking execution as complete");
          save(getExecution()
              .setStatus(WorkflowExecutionStatus.COMPLETE.ordinal())
              .setEndTime(System.currentTimeMillis())
          );
        }

        LOG.info("Stopping attempt: " + attempt);
        if (attempt.getStatus() == AttemptStatus.FAIL_PENDING.ordinal()) {
          attempt.setStatus(AttemptStatus.FAILED.ordinal());
        } else if (attempt.getStatus() == AttemptStatus.SHUTDOWN_PENDING.ordinal()) {
          attempt.setStatus(AttemptStatus.SHUTDOWN.ordinal());
        } else if (attempt.getStatus() == AttemptStatus.FAILED.ordinal()) {
          LOG.info("Attempt was already stopped (via shutdown hook probably)");
        } else {
          attempt.setStatus(AttemptStatus.FINISHED.ordinal());
        }

      }

      LOG.info("Marking status of attempt " + attempt.getId() + " as " + attempt.getStatus());

      save(attempt);

    }

    heartbeatThread.interrupt();
    try {
      heartbeatThread.join();
    } catch (InterruptedException e) {
      LOG.error("Failed to interrupt heartbeat thread!");
    }
  }


  public synchronized WorkflowAttempt getAttempt() throws IOException {
    return rldb.workflowAttempts().find(workflowAttemptId);
  }

  public synchronized WorkflowExecution getExecution() throws IOException {
    return getAttempt().getWorkflowExecution();
  }

  public IRlDb getDb() {
    return rldb;
  }


  protected AlertsHandler getProvidedHandler() {
    return providedHandler;
  }


  protected synchronized void save(WorkflowExecution execution) throws IOException {
    rldb.workflowExecutions().save(execution);
  }

  protected synchronized void save(WorkflowAttempt attempt) throws IOException {
    rldb.workflowAttempts().save(attempt);
  }

  private class Heartbeat implements Runnable {
    @Override
    public void run() {
      //noinspection InfiniteLoopStatement
      while (!Thread.interrupted()) {
        try {
          Thread.sleep(HEARTBEAT_INTERVAL);
          heartbeat();
        } catch (InterruptedException e) {
          LOG.info("Heartbeat thread killed");
          return;
        }
      }
    }
  }

  private synchronized void heartbeat() {
    try {
      save(getAttempt()
          .setLastHeartbeat(System.currentTimeMillis())
      );
    } catch (IOException e) {
      LOG.error("Failed to record heartbeat: ", e);
      throw new RuntimeException(e);
    }
  }

}
