package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlert;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.DbPersistence;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowQueries;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

public class DiedUnclean implements ExecutionAlertGenerator {

  private static final int MISSED_HEARTBEATS_THRESHOLD = DbPersistence.NUM_HEARTBEAT_TIMEOUTS * 5; // 5 min, to reduce false alarms

  @Override
  public List<ExecutionAlert> generateAlerts(IDatabases db) throws IOException {

    List<ExecutionAlert> alerts = Lists.newArrayList();

    for (WorkflowExecution execution : WorkflowQueries.getDiedUncleanExecutions(db, null, 7, MISSED_HEARTBEATS_THRESHOLD)) {
      long id = execution.getId();
      alerts.add(new ExecutionAlert(id,
          "Execution has died without shutting down cleanly.  This often means the process was killed by the system OOM killer.  Please cancel or resume the execution.",
              WorkflowRunnerNotification.DIED_UNCLEAN)
      );
    }

    return alerts;
  }

}
