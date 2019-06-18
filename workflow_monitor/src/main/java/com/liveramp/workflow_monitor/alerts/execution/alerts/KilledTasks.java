package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.util.Properties;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
import com.liveramp.workflow_monitor.alerts.execution.thresholds.GreaterThan;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

public class KilledTasks extends JobThresholdAlert {

  private static final String PROPERTIES_PREFIX = "alert." + KilledTasks.class.getSimpleName();

  private static final String MIN_TASKS_PROP = PROPERTIES_PREFIX+".min_tasks";
  private static final String MIN_TASKS_DEFAULT = "20";

  private static final String KILL_THRESHOLD_PROP = PROPERTIES_PREFIX+".threshold";
  private static final String KILL_THRESHOLD_DEFAULT = ".5";

  private static final String GROUP = "org.apache.hadoop.mapreduce.JobCounter";
  private static final String KILLED_MAPS = "NUM_KILLED_MAPS";
  private static final String KILLED_REDUCES = "NUM_KILLED_REDUCES";
  private static final String LAUNCHED_MAPS = "TOTAL_LAUNCHED_MAPS";
  private static final String LAUNCHED_REDUCES = "TOTAL_LAUNCHED_REDUCES";

  private final long minTasks;

  public static KilledTasks create(Properties properties) {
    return new KilledTasks(
        Double.parseDouble(properties.getProperty(KILL_THRESHOLD_PROP, KILL_THRESHOLD_DEFAULT)),
        Long.parseLong(properties.getProperty(MIN_TASKS_PROP, MIN_TASKS_DEFAULT))
    );
  }

  private KilledTasks(double threshold, long minTasks) {
    super(
        threshold,
        WorkflowRunnerNotification.PERFORMANCE,
        new MultimapBuilder<String, String>()
            .put(GROUP, KILLED_MAPS)
            .put(GROUP, KILLED_REDUCES)
            .put(GROUP, LAUNCHED_MAPS)
            .put(GROUP, LAUNCHED_REDUCES)
            .get(),
        new GreaterThan());

    this.minTasks = minTasks;

  }

  @Override
  protected Double calculateStatistic(MapreduceJob job, TwoNestedMap<String, String, Long> counters) {

    long killed = get(GROUP, KILLED_MAPS, counters) + get(GROUP, KILLED_REDUCES, counters);
    long launched = get(GROUP, LAUNCHED_MAPS, counters) + get(GROUP, LAUNCHED_REDUCES, counters);

    if (launched < minTasks) {
      return null;
    }

    return ((double)killed) / ((double)launched);

  }

  @Override
  protected String getMessage(double value, MapreduceJob job, TwoNestedMap<String, String, Long> counters) {
    return asPercent(value) + " of launched tasks were killed.  This may indicate heavy contention and under-allocated pools.";
  }
}
