package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlert;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowExecutionStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowQueries;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

public class KilledTasks implements ExecutionAlertGenerator {
  @Override
  public List<ExecutionAlert> generateAlerts(IDatabases db) throws IOException {

    List<ExecutionAlert> alerts = Lists.newArrayList();

    for (WorkflowExecution execution : WorkflowQueries.queryWorkflowExecutions(db, null, null, null, null, System.currentTimeMillis() - 7 * 24 * 60 * 60 * 1000, null,
        WorkflowExecutionStatus.INCOMPLETE, null)) {

      TwoNestedMap<String, String, Long> counters = WorkflowQueries.getFlatCounters(db.getRlDb(), execution.getId());

      long killed =
          counters.get("org.apache.hadoop.mapreduce.JobCounter", "NUM_KILLED_MAPS") +
          counters.get("org.apache.hadoop.mapreduce.JobCounter", "NUM_KILLED_REDUCES");

      long launched =
          counters.get("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_MAPS") +
          counters.get("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_REDUCES");

      if (killed > .5 * launched) {
        alerts.add(new ExecutionAlert(execution.getId(),
            "There were " + killed + " killed tasks out of " + launched + " launched tasks.  This may indicate heavy contention and under-allocated pools.",
            WorkflowRunnerNotification.PERFORMANCE
        ));
      }

    }

    return alerts;
  }
}
