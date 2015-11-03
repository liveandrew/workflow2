package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.MapreduceJobAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

public class KilledTasks implements MapreduceJobAlertGenerator {

  @Override
  public List<AlertMessage> generateAlerts(MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException {

    long killed =
        counters.get("org.apache.hadoop.mapreduce.JobCounter", "NUM_KILLED_MAPS") +
            counters.get("org.apache.hadoop.mapreduce.JobCounter", "NUM_KILLED_REDUCES");

    long launched =
        counters.get("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_MAPS") +
            counters.get("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_REDUCES");

    if (killed > .5 * launched) {
      return Lists.newArrayList(new AlertMessage(
          "There were " + killed + " killed tasks out of " + launched + " launched tasks.  This may indicate heavy contention and under-allocated pools.",
          WorkflowRunnerNotification.PERFORMANCE
      ));
    }

    return Lists.newArrayList();
  }
}
