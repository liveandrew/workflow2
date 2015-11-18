package com.liveramp.workflow_monitor.alerts.execution.alerts;

import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.JobThresholdAlert;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

public class CPUUsage extends JobThresholdAlert {

  private static final double CPU_USED_RATIO = 2.0;

  private static final Multimap<String, String> COUNTERS = new MultimapBuilder<String, String>()
      .put(JOB_COUNTER_GROUP, VCORES_MAPS)
      .put(JOB_COUNTER_GROUP, VCORES_REDUCES)
      .put(TASK_COUNTER_GROUP, CPU_MILLISECONDS)
      .get();

  public CPUUsage() {
    super(CPU_USED_RATIO, WorkflowRunnerNotification.PERFORMANCE, COUNTERS);
  }

  @Override
  protected Double calculateStatistic(TwoNestedMap<String, String, Long> counters) {
    Long mapAllocatedCore = get(JOB_COUNTER_GROUP, VCORES_MAPS, counters);
    Long reduceAllocatedCore = get(JOB_COUNTER_GROUP, VCORES_REDUCES, counters);

    Long cpuMillis = counters.get(TASK_COUNTER_GROUP, CPU_MILLISECONDS);
    long allMillis = mapAllocatedCore + reduceAllocatedCore;

    if (allMillis == 0 || cpuMillis == null) {
      return null;
    }

    return cpuMillis.doubleValue() / ((double)allMillis);

  }

  @Override
  protected String getMessage(double value) {
    return "This job used "+asPercent(value)+" of its allocated CPU time.  This is probably due to high Garbage Collection time, " +
        "unless the job is doing explicit multi-threading.  If CPU  time is due to GC, try to reduce either object creation or increase memory.  " +
        "If you are explicitly multi-threading, please increase set mapreduce.map.cpu.vcores or mapreduce.reduce.cpu.vcores accordingly.";
  }
}
