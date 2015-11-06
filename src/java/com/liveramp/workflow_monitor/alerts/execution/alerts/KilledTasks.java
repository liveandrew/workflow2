package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.MapreduceJobAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

public class KilledTasks extends MapreduceJobAlertGenerator {

  private static final String GROUP = "org.apache.hadoop.mapreduce.JobCounter";
  private static final String KILLED_MAPS = "NUM_KILLED_MAPS";
  private static final String KILLED_REDUCES = "NUM_KILLED_REDUCES";
  private static final String LAUNCHED_MAPS = "TOTAL_LAUNCHED_MAPS";
  private static final String LAUNCHED_REDUCES = "TOTAL_LAUNCHED_REDUCES";

  public KilledTasks() {
    super(new MultimapBuilder<String, String>()
        .put(GROUP, KILLED_MAPS)
        .put(GROUP, KILLED_REDUCES)
        .put(GROUP, LAUNCHED_MAPS)
        .put(GROUP, LAUNCHED_REDUCES)
        .get());
  }

  @Override
  public List<AlertMessage> generateAlerts(MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException {

    long killed =
        counters.get(GROUP, KILLED_MAPS) +
            counters.get(GROUP, KILLED_REDUCES);

    long launched =
        counters.get(GROUP, LAUNCHED_MAPS) +
            counters.get(GROUP, LAUNCHED_REDUCES);

    if (killed > .5 * launched) {
      return Lists.newArrayList(new AlertMessage(
          "There were " + killed + " killed tasks out of " + launched + " launched tasks.  This may indicate heavy contention and under-allocated pools.",
          WorkflowRunnerNotification.PERFORMANCE
      ));
    }

    return Lists.newArrayList();
  }
}
