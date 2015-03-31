package com.rapleaf.cascading_ext.workflow2.action_operations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.RunningJob;

import cascading.flow.Flow;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;

import com.liveramp.cascading_ext.counters.Counter;
import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.java_support.event_timer.FixedTimedEvent;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.workflow2.ActionOperation;
import com.rapleaf.cascading_ext.workflow2.EmptyListener;
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

    final AtomicBoolean isComplete = new AtomicBoolean(false);

    flow.addListener(new EmptyListener(){
      @Override
      public void onCompleted(Flow flow) {
        isComplete.set(true);
      }
    });

    flow.complete();

    if(!isComplete.get()){
      throw new RuntimeException("Flow terminated but did not complete!  Possible shutdown hook invocation.");
    }

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
  public ThreeNestedMap<String, String, String, Long> getJobCounters(){
    return Counters.getCounterMap(flow.getFlowStats());
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
