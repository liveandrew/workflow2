package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.time.Duration;

import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
import com.liveramp.workflow_monitor.alerts.execution.thresholds.LessThan;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

public class ShortMaps extends JobThresholdAlert {
  protected static final Multimap<String, String> REQUIRED_COUNTERS = new MultimapBuilder<String, String>()
      .put(JOB_COUNTER_GROUP, LAUNCHED_MAPS)
      .get();

  protected static final double TASK_TIME_THRESHOLD = Duration.ofMinutes(1).toMillis();
  protected static final double MIN_NUM_THRESHOLD = 5;

  public ShortMaps() {
    super(TASK_TIME_THRESHOLD, WorkflowRunnerNotification.PERFORMANCE, REQUIRED_COUNTERS, new LessThan());
  }

  @Override
  protected Double calculateStatistic(MapreduceJob job, TwoNestedMap<String, String, Long> counters) {
    Double avgDuration = job.getAvgMapDuration() == null ? null : job.getAvgMapDuration().doubleValue();
    Long launchedMaps = counters.get(JOB_COUNTER_GROUP, LAUNCHED_MAPS);
    return (launchedMaps != null && launchedMaps > MIN_NUM_THRESHOLD) ? avgDuration : null;
  }

  @Override
  protected String getMessage(double value) {
    return "Maps in this job take " + value / 1000 + "seconds on average, which is wastefully short. Consider batching more aggressively.";
  }
}
