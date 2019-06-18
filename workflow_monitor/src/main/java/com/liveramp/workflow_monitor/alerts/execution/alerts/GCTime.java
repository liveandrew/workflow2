package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.util.Properties;

import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
import com.liveramp.workflow_monitor.alerts.execution.thresholds.GreaterThan;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

import static com.liveramp.workflow_core.WorkflowConstants.WORKFLOW_ALERT_RECOMMENDATIONS;

public class GCTime extends JobThresholdAlert {

  private static final String PROPERTIES_PREFIX = "alert." + GCTime.class.getSimpleName();

  private static final String GC_FRACTION_THRESHOLD_PROP = PROPERTIES_PREFIX+".gc_fraction_threshold";
  private static final String GC_FRACTION_THRESHOLD_DEFAULT = ".25";

  //  unclear why, but there seems to be some fixed startup GC time we probably want to ignore in tiny jobs
  private static final String GC_STARTUP_TIME_PROP = PROPERTIES_PREFIX+".gc_startup_time_ms";
  private static final String GC_STARTUP_TIME_DEFAULT = "3000";

  private static final String PREAMBLE = " of processing time was spent in Garbage Collection. ";

  private final long gcStartupTime;

  private static final Multimap<String, String> REQUIRED_COUNTERS = new MultimapBuilder<String, String>()
      .put(TASK_COUNTER_GROUP, GC_TIME_MILLIS)
      .put(JOB_COUNTER_GROUP, MILLIS_MAPS)
      .put(JOB_COUNTER_GROUP, MILLIS_REDUCES).get();

  public static GCTime create(Properties properties){
    return new GCTime(
        Double.parseDouble(properties.getProperty(GC_FRACTION_THRESHOLD_PROP, GC_FRACTION_THRESHOLD_DEFAULT)),
        Long.parseLong(properties.getProperty(GC_STARTUP_TIME_PROP, GC_STARTUP_TIME_DEFAULT))
    );
  }


  public GCTime(double gcFraction, long gcStartupTime) {
    super(gcFraction, WorkflowRunnerNotification.PERFORMANCE, REQUIRED_COUNTERS, new GreaterThan());

    this.gcStartupTime = gcStartupTime;

  }

  @Override
  protected Double calculateStatistic(MapreduceJob job, TwoNestedMap<String, String, Long> counters) {

    Long gcTime = counters.get(TASK_COUNTER_GROUP, GC_TIME_MILLIS);

    Long launchedMaps = get(JOB_COUNTER_GROUP, LAUNCHED_MAPS, counters);
    Long launchedReduces = get(JOB_COUNTER_GROUP, LAUNCHED_REDUCES, counters);

    Long totalForgivenGC = (launchedMaps + launchedReduces) * gcStartupTime;

    if (gcTime == null) {
      return null;
    }

    Long allTime = get(JOB_COUNTER_GROUP, MILLIS_MAPS, counters) + get(JOB_COUNTER_GROUP, MILLIS_REDUCES, counters);

    return (gcTime.doubleValue() - totalForgivenGC) / allTime.doubleValue();
  }

  @Override
  protected String getMessage(double value, MapreduceJob job, TwoNestedMap<String, String, Long> counters) {
    return asPercent(value) + PREAMBLE + WORKFLOW_ALERT_RECOMMENDATIONS.get("GCTime");
  }
}
