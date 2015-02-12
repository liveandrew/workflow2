package com.rapleaf.cascading_ext.workflow2.action_operations;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.RunningJob;

import cascading.flow.Flow;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;

import com.liveramp.cascading_ext.counters.Counter;
import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.java_support.event_timer.FixedTimedEvent;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.workflow2.ActionOperation;
import com.rapleaf.cascading_ext.workflow2.Step;

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
  public List<RunningJob> listJobs() {

    List<RunningJob> jobs = Lists.newArrayList();

    try {
      for (FlowStepStats st : flow.getFlowStats().getFlowStepStats()) {
        HadoopStepStats hdStepStats = (HadoopStepStats)st;
        RunningJob job = hdStepStats.getRunningJob();
        jobs.add(job);
      }
    } catch (NullPointerException e) {
      // getJobID on occasion throws a null pointer exception, ignore it
    }

    return jobs;
  }

  @Override
  public void timeOperation(Step.StepTimer stepTimer, String checkpointToken, List<NestedCounter> nestedCounters) {
    Map<FlowStepStats, List<Counter>> counters = Counters.getCountersByStep(flow);

    // add timers and counters from flows the action executed
    for (FlowStepStats stepStats : flow.getFlowStats().getFlowStepStats()) {
      stepTimer.addChild(new FixedTimedEvent(stepStats.getName(), stepStats.getStartTime(), stepStats.getFinishedTime()));

      if (counters.containsKey(stepStats)) {
        for (Counter c : counters.get(stepStats)) {
          NestedCounter nc = new NestedCounter(c);
          nestedCounters.add(nc);
        }
      }
    }
  }
}
