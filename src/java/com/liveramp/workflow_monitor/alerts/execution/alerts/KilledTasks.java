package com.liveramp.workflow_monitor.alerts.execution.alerts;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

public class KilledTasks extends JobThresholdAlert {

  private static final String GROUP = "org.apache.hadoop.mapreduce.JobCounter";
  private static final String KILLED_MAPS = "NUM_KILLED_MAPS";
  private static final String KILLED_REDUCES = "NUM_KILLED_REDUCES";
  private static final String LAUNCHED_MAPS = "TOTAL_LAUNCHED_MAPS";
  private static final String LAUNCHED_REDUCES = "TOTAL_LAUNCHED_REDUCES";

  public KilledTasks() {
    super(
        .5,
        WorkflowRunnerNotification.PERFORMANCE,
        new MultimapBuilder<String, String>()
        .put(GROUP, KILLED_MAPS)
        .put(GROUP, KILLED_REDUCES)
        .put(GROUP, LAUNCHED_MAPS)
        .put(GROUP, LAUNCHED_REDUCES)
        .get());
  }


  @Override
  protected double calculateStatistic(TwoNestedMap<String, String, Long> counters) {

    long killed =
        counters.get(GROUP, KILLED_MAPS) +
            counters.get(GROUP, KILLED_REDUCES);

    long launched =
        counters.get(GROUP, LAUNCHED_MAPS) +
            counters.get(GROUP, LAUNCHED_REDUCES);

    return ((double) killed) / ((double) launched);

  }

  @Override
  protected String getMessage(double value) {
    return (value*100) +"% of launched tasks were killed.  This may indicate heavy contention and under-allocated pools.";
  }
}
