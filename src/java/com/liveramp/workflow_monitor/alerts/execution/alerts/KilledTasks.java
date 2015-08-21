package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlert;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowExecutionStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowQueries;

public class KilledTasks implements ExecutionAlertGenerator {
  @Override
  public List<ExecutionAlert> generateAlerts(IDatabases db) throws IOException {

    List<WorkflowExecution> executions = WorkflowQueries.queryWorkflowExecutions(db,
        null,
        null,
        null,
        null,
        System.currentTimeMillis() - 7 * 24 * 60 * 60 * 1000,
        null,
        WorkflowExecutionStatus.INCOMPLETE
    );

    List<ExecutionAlert> alerts = Lists.newArrayList();

    for (WorkflowExecution execution : executions) {

      TwoNestedMap<String, String, Long> counters = WorkflowQueries.getFlatCounters(db.getRlDb(), execution.getId());

      Long launchedMaps = counters.get("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_MAPS");
      Long launchedReduces = counters.get("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_REDUCES");

      Long killedMaps = counters.get("org.apache.hadoop.mapreduce.JobCounter", "NUM_KILLED_MAPS");
      Long killedReduces = counters.get("org.apache.hadoop.mapreduce.JobCounter", "NUM_KILLED_REDUCES");

      long killed = killedMaps + killedReduces;
      long launched = launchedMaps + launchedReduces;

      if (killed > .5 * launched) {
        alerts.add(new ExecutionAlert(execution.getId(),
            "There were " + killed + " killed tasks out of " + launched + " launched tasks.  This may indicate heavy contention and under-allocated pools.",
            AlertSeverity.INFO
        ));
      }

    }

    return alerts;
  }
}
