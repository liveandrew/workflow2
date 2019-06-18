package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.time.Duration;
import java.util.Properties;

import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
import com.liveramp.workflow_monitor.alerts.execution.thresholds.LessThan;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

import static com.liveramp.workflow_core.WorkflowConstants.WORKFLOW_ALERT_RECOMMENDATIONS;

public class ShortReduces extends JobThresholdAlert {

  private static final String PROPERTIES_PREFIX = "alert." + ShortReduces.class.getSimpleName();

  private static final String REDUCE_TIME_LIMIT_PROP = PROPERTIES_PREFIX+".reduce_time_limit";
  private static final String REDUCE_TIME_LIMIT_DEFAULT = "120000";

  protected static final Multimap<String, String> REQUIRED_COUNTERS = new MultimapBuilder<String, String>()
      .put(JOB_COUNTER_GROUP, LAUNCHED_REDUCES)
      .get();

  protected static final double MIN_NUM_THRESHOLD = 1;

  public static ShortReduces create(Properties properties){
    return new ShortReduces(Double.parseDouble(properties.getProperty(REDUCE_TIME_LIMIT_PROP, REDUCE_TIME_LIMIT_DEFAULT)));
  }

  public ShortReduces(double threshold) {
    super(threshold, WorkflowRunnerNotification.PERFORMANCE, REQUIRED_COUNTERS, new LessThan());
  }

  @Override
  protected Double calculateStatistic(MapreduceJob job, TwoNestedMap<String, String, Long> counters) {
    Double avgDuration = job.getAvgReduceDuration() == null ? null : job.getAvgReduceDuration().doubleValue();
    Long launchedReduces = counters.get(JOB_COUNTER_GROUP, LAUNCHED_REDUCES);
    return (launchedReduces != null && launchedReduces > MIN_NUM_THRESHOLD) ? avgDuration : null;
  }

  @Override
  protected String getMessage(double value, MapreduceJob job, TwoNestedMap<String, String, Long> counters) {
    return "Reduces in this job take " + value / 1000 + " seconds on average, which is wastefully short (total " +
        counters.get(JOB_COUNTER_GROUP, LAUNCHED_REDUCES) + " reduces)" + WORKFLOW_ALERT_RECOMMENDATIONS.get("ShortReduces");
  }
}
