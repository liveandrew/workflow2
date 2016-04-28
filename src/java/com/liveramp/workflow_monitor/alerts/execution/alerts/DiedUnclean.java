package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.Collection;

import com.google.common.base.Optional;

import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.liveramp.workflow_state.DbPersistence;
import com.liveramp.workflow_state.ProcessStatus;
import com.liveramp.workflow_state.WorkflowQueries;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

public class DiedUnclean implements ExecutionAlertGenerator {

  private static final int MISSED_HEARTBEATS_THRESHOLD = DbPersistence.NUM_HEARTBEAT_TIMEOUTS * 5; // 5 min, to reduce false alarms

  @Override
  public AlertMessage generateAlert(long fetchTime, WorkflowExecution execution, Collection<WorkflowAttempt> attempts) throws IOException {

    Optional<WorkflowAttempt> lastAttempt = WorkflowQueries.getLatestAttemptOptional(attempts);

    if (lastAttempt.isPresent()) {
      ProcessStatus process = WorkflowQueries.getProcessStatus(fetchTime, lastAttempt.get(), execution, MISSED_HEARTBEATS_THRESHOLD);
      if (process == ProcessStatus.TIMED_OUT) {
        return new AlertMessage(
            "Execution has died without shutting down cleanly.  This often means the process was killed by the system OOM killer.  Please cancel or resume the execution.",
            WorkflowRunnerNotification.DIED_UNCLEAN
        );
      }
    }

    return null;

  }

}
