package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.time.Duration;
import java.util.Properties;

import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
import com.liveramp.workflow_monitor.alerts.execution.thresholds.GreaterThan;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

import static com.liveramp.workflow_core.WorkflowConstants.WORKFLOW_ALERT_RECOMMENDATIONS;

public class CPUUsage extends JobThresholdAlert {
  private static final Logger LOG = LoggerFactory.getLogger(CPUUsage.class);

  private static final String PROPERTIES_PREFIX = "alert." + CPUUsage.class.getSimpleName();

  private static final String CPU_USED_RATIO_PROP = PROPERTIES_PREFIX + ".cpu_used_ratio";
  private static final String DEFAULT_CPU_USED_RATIO = "2.0";

  private static final String MIN_TIME_THRESHOLD_PROP = PROPERTIES_PREFIX + ".min_time_threshold_ms";
  private static final String DEFAULT_MIN_TIME_THRESHOLD = Long.toString(Duration.ofHours(3).toMillis());

  private static final String TASK_FORGIVENESS_PROP = PROPERTIES_PREFIX + ".task_forgiveness_ms";
  private static final String DEFAULT_TASK_FORGIVENESS = Long.toString(5 * 1000);

  private static final String PREAMBLE = " of allocated CPU time was used. ";


  private final long minTimeThreshold;
  private final long taskForgiveness;

  private static final Multimap<String, String> COUNTERS = new MultimapBuilder<String, String>()
      .put(JOB_COUNTER_GROUP, VCORES_MAPS)
      .put(JOB_COUNTER_GROUP, VCORES_REDUCES)
      .put(TASK_COUNTER_GROUP, CPU_MILLISECONDS)
      .get();

  public static CPUUsage create(Properties properties) {
    return new CPUUsage(
        Double.parseDouble(properties.getProperty(CPU_USED_RATIO_PROP, DEFAULT_CPU_USED_RATIO)),
        Long.parseLong(properties.getProperty(MIN_TIME_THRESHOLD_PROP, DEFAULT_MIN_TIME_THRESHOLD)),
        Long.parseLong(properties.getProperty(TASK_FORGIVENESS_PROP, DEFAULT_TASK_FORGIVENESS))
    );
  }

  private CPUUsage(double cpuRatio, long minTimeThreshold, long taskForgiveness) {
    super(cpuRatio, WorkflowRunnerNotification.PERFORMANCE, COUNTERS, new GreaterThan());

    this.minTimeThreshold = minTimeThreshold;
    this.taskForgiveness = taskForgiveness;
  }

  @Override
  protected Double calculateStatistic(MapreduceJob job, TwoNestedMap<String, String, Long> counters) {
    Long mapAllocatedCore = get(JOB_COUNTER_GROUP, VCORES_MAPS, counters);
    Long reduceAllocatedCore = get(JOB_COUNTER_GROUP, VCORES_REDUCES, counters);

    Long launchedMaps = get(JOB_COUNTER_GROUP, LAUNCHED_MAPS, counters);
    Long launchedReduces = get(JOB_COUNTER_GROUP, LAUNCHED_REDUCES, counters);

    Long cpuMillis = counters.get(TASK_COUNTER_GROUP, CPU_MILLISECONDS);
    long allocatedMillis = mapAllocatedCore + reduceAllocatedCore;

    if (allocatedMillis == 0 || cpuMillis == null) {
      return null;
    }

    //  ignore small startup time CPU factors so we don't alert on tiny jobs
    if (cpuMillis < minTimeThreshold) {
      LOG.debug("Skipping alert for < " + minTimeThreshold + " ms: " + job.getJobIdentifier());
      return null;
    }

    Long forgivenCPU = (launchedMaps + launchedReduces) * taskForgiveness;

    return (cpuMillis.doubleValue() - forgivenCPU) / ((double)allocatedMillis);

  }

  @Override
  protected String getMessage(double value, MapreduceJob job, TwoNestedMap<String, String, Long> counters) {
    return asPercent(value) + PREAMBLE + WORKFLOW_ALERT_RECOMMENDATIONS.get("CPUUsage");
  }
}
