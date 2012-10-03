package com.rapleaf.cascading_ext.workflow2.action_operations;

import cascading.flow.Flow;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;
import com.liveramp.cascading_ext.counters.Counter;
import com.liveramp.cascading_ext.counters.Counters;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.workflow2.ActionOperation;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.support.event_timer.FixedTimedEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public String getProperty(String propertyName) {
    return flow.getProperty(propertyName);
  }

  @Override
  public int getProgress(int maxPct) {
    FlowStats flowstats = flow.getFlowStats();
    int numComplete = 0;
    List<FlowStepStats> stepStatsList = flowstats.getFlowStepStats();

    for (FlowStepStats stepStats : stepStatsList) {
      if (stepStats.isFinished()) {
        numComplete++;
      }
    }

    return (int) ((double) numComplete / flowstats.getStepsCount() * maxPct);
  }

  @Override
  public String getName() {
    return flow.getName();
  }

  @Override
  public Map<String, String> getSubStepIdToName(int operationIndex) {
    Map<String, String> subStepIdToName = new HashMap<String, String>();

    int count = 1;
    for (FlowStepStats st : flow.getFlowStats().getFlowStepStats()) {
      HadoopStepStats hdStepStats = (HadoopStepStats) st;

      try {
        String stepId = hdStepStats.getJobID();
        String name = "Flow " + Integer.toString(operationIndex) + " (" + count + "/" + flow.getFlowStats().getFlowStepStats().size() + ")";
        subStepIdToName.put(stepId, name);
      } catch (NullPointerException e) {
        // getJobID on occasion throws a null pointer exception, ignore it
      }

      count++;
    }

    return subStepIdToName;
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
