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

public class ShortMaps extends JobThresholdAlert {
  protected static final Multimap<String, String> REQUIRED_COUNTERS = new MultimapBuilder<String, String>()
      .put(JOB_COUNTER_GROUP, LAUNCHED_MAPS)
      .get();

  private static final String PROPERTIES_PREFIX = "alert." + ShortMaps.class.getSimpleName();

  private static final String MAP_TIME_LIMIT_PROP = PROPERTIES_PREFIX+".map_time_limit";
  private static final String MAP_TIME_LIMIT_DEFAULT = "120000";

  protected static final double MIN_NUM_THRESHOLD = 1;

  public static ShortMaps create(Properties properties){
    return new ShortMaps(Double.parseDouble(properties.getProperty(MAP_TIME_LIMIT_PROP, MAP_TIME_LIMIT_DEFAULT)));
  }

  private ShortMaps(double threshold) {
    super(threshold, WorkflowRunnerNotification.PERFORMANCE, REQUIRED_COUNTERS, new LessThan());
  }

  @Override
  protected Double calculateStatistic(MapreduceJob job, TwoNestedMap<String, String, Long> counters) {
    Double avgDuration = job.getAvgMapDuration() == null ? null : job.getAvgMapDuration().doubleValue();
    Long launchedMaps = counters.get(JOB_COUNTER_GROUP, LAUNCHED_MAPS);

    if(avgDuration == null){
      return null;
    }

    //  0 seconds is probably a record error
    if(avgDuration == 0){
      return null;
    }

    return launchedMaps != null && launchedMaps > MIN_NUM_THRESHOLD ? avgDuration : null;
  }

  @Override
  protected String getMessage(double value, MapreduceJob job, TwoNestedMap<String, String, Long> counters) {
    return "Maps in this job take " + value / 1000 + " seconds on average, which is wastefully short (total " +
        counters.get(JOB_COUNTER_GROUP, LAUNCHED_MAPS) + " maps) " + WORKFLOW_ALERT_RECOMMENDATIONS.get("ShortMaps");
  }
}
