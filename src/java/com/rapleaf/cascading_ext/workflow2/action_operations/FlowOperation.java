package com.rapleaf.cascading_ext.workflow2.action_operations;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.RunningJob;

import cascading.flow.Flow;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;

import com.liveramp.cascading_ext.counters.Counter;
import com.liveramp.cascading_ext.counters.Counters;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.workflow2.ActionOperation;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.liveramp.cascading_ext.event_timer.FixedTimedEvent;

public class FlowOperation implements ActionOperation {
  private final Flow flow;

  public FlowOperation(Flow flow) {
    this.flow = flow;
  }

  @Override
  public void start() {
    flow.start();
  }

  @Override
  public void complete() {
    flow.complete();
  }

  @Override
  public String getName() {
    return flow.getName();
  }

  @Override
  public Map<String, String> getSubStepStatusLinks() {
    Map<String, String> subSteps = new LinkedHashMap<String, String>();

    try {
      for (FlowStepStats st : flow.getFlowStats().getFlowStepStats()) {
        HadoopStepStats hdStepStats = (HadoopStepStats)st;
        RunningJob job = hdStepStats.getRunningJob();
        subSteps.put(hdStepStats.getStatusURL(), job.getJobName());
      }
    } catch (NullPointerException e) {
      // getJobID on occasion throws a null pointer exception, ignore it
    }

    return subSteps;
  }

  @Override
  public void timeOperation(Step.StepTimer stepTimer, String checkpointToken, List<NestedCounter> nestedCounters) {
    Map<FlowStepStats, List<Counter>> counters = Counters.getCountersByStep(flow);

    // add timers and counters from flows the action executed
    for (FlowStepStats stepStats : flow.getFlowStats().getFlowStepStats()) {
      stepTimer.addChild(new FixedTimedEvent(stepStats.getName(), stepStats.getStartTime(), stepStats.getFinishedTime()));

      if (counters.containsKey(stepStats)) {
        for (Counter c : counters.get(stepStats)) {
          NestedCounter nc = new NestedCounter(c, stepStats.getName());
          nc.addParentEvent(checkpointToken);
          nestedCounters.add(nc);
        }
      }
    }
  }
}
