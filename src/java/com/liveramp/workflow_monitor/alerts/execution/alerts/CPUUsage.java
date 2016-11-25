package com.liveramp.workflow_monitor.alerts.execution.alerts;

import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

public class CPUUsage extends JobThresholdAlert {
  private static final Logger LOG = LoggerFactory.getLogger(CPUUsage.class);

  private static final double CPU_USED_RATIO = 2.0;

  private static final long MIN_TIME_ALERT_THRESHOLD = 3 * 60 * 1000;

  private static final long TASK_FORGIVENESS = 15 * 1000;


  private static final Multimap<String, String> COUNTERS = new MultimapBuilder<String, String>()
      .put(JOB_COUNTER_GROUP, VCORES_MAPS)
      .put(JOB_COUNTER_GROUP, VCORES_REDUCES)
      .put(TASK_COUNTER_GROUP, CPU_MILLISECONDS)
      .get();

  public CPUUsage() {
    super(CPU_USED_RATIO, WorkflowRunnerNotification.PERFORMANCE, COUNTERS);
  }

  @Override
  protected Double calculateStatistic(String jobIdentifier, TwoNestedMap<String, String, Long> counters) {
    Long mapAllocatedCore = get(JOB_COUNTER_GROUP, VCORES_MAPS, counters);
    Long reduceAllocatedCore = get(JOB_COUNTER_GROUP, VCORES_REDUCES, counters);

    Long launchedMaps = counters.get(JOB_COUNTER_GROUP, LAUNCHED_MAPS);
    Long launchedReduces = counters.get(JOB_COUNTER_GROUP, LAUNCHED_REDUCES);

    Long cpuMillis = counters.get(TASK_COUNTER_GROUP, CPU_MILLISECONDS);
    long allMillis = mapAllocatedCore + reduceAllocatedCore;

    if (allMillis == 0 || cpuMillis == null) {
      return null;
    }


    //  ignore small startup time CPU factors so we don't alert on tiny jobs
    if (cpuMillis < MIN_TIME_ALERT_THRESHOLD) {
      return null;
    }

    if (cpuMillis < (launchedMaps + launchedReduces) * TASK_FORGIVENESS) {
      LOG.info("Skipping because job "+jobIdentifier+" was under min task time threshold");
    }

    return cpuMillis.doubleValue() / ((double)allMillis);

  }

  @Override
  protected String getMessage(double value) {
    return "This job used " + asPercent(value) + " of its allocated CPU time.  This is probably due to high Garbage Collection time, " +
        "unless the job is doing explicit multi-threading.  If CPU  time is due to GC, try to reduce either object creation or increase memory.  " +
        "If you are explicitly multi-threading, please increase set mapreduce.map.cpu.vcores or mapreduce.reduce.cpu.vcores accordingly.";
  }
}
