package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.Flow;
import cascading.stats.StepStats;

import com.rapleaf.cascading_ext.counters.Counter;
import com.rapleaf.cascading_ext.counters.Counters;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.support.event_timer.EventTimer;
import com.rapleaf.support.event_timer.FixedTimedEvent;

import java.util.*;

public final class Step {

  private String checkpointTokenPrefix = "";
  private final Action action;
  private final Set<Step> dependencies;
  private final StepTimer timer = new StepTimer();
  private final List<NestedCounter> nestedCounters = new ArrayList<NestedCounter>();

  public class StepTimer extends EventTimer {

    public StepTimer() {
      super(null);
    }

    @Override
    public String getEventName() {
      return getSimpleCheckpointToken();
    }
  }

  public Step(Action action) {
    this.action = action;
    dependencies = Collections.emptySet();
  }

  public Step(Action action, Step previous, Step... rest) {
    this.action = action;
    dependencies = new HashSet<Step>(Arrays.asList(rest));
    dependencies.add(previous);
    if (dependencies.contains(null)) {
      throw new NullPointerException("null cannot be a dependency for a step!");
    }
  }

  public Step(Action action, List<Step> steps) {
    this.action = action;
    dependencies = new HashSet<Step>(steps);
    if (dependencies.contains(null)) {
      throw new NullPointerException("null cannot be a dependency for a step!");
    }
  }

  public Set<Step> getDependencies() {
    return dependencies;
  }

  public Action getAction() {
    return action;
  }

  public void setCheckpointTokenPrefix(String checkpointTokenPrefix) {
    this.checkpointTokenPrefix = checkpointTokenPrefix;
  }

  public String getCheckpointTokenPrefix() {
    return checkpointTokenPrefix;
  }

  public String getCheckpointToken() {
    return getCheckpointTokenPrefix() + getAction().getCheckpointToken();
  }

  public String getSimpleCheckpointToken() {
    return getAction().getCheckpointToken();
  }

  @Override
  public String toString() {
    return "Step " + checkpointTokenPrefix + " " + action + " deps=" + dependencies;
  }

  public StepTimer getTimer() {
    return timer;
  }
  
  public List<NestedCounter> getCounters() { 
    return nestedCounters;
  }

  public void run() {
    timer.start();
    try {
      action.internalExecute();
    } finally {
      for (Flow flow : action.getRunFlows()) {
        Map<StepStats, List<Counter>> counters = Counters.getCountersByStep(flow);
        
        // add timers and counters from flows the action executed
        for (StepStats stepStats : flow.getFlowStats().getStepStats()) {
          timer.addChild(new FixedTimedEvent(stepStats.getName(), stepStats.getStartTime(), stepStats.getFinishedTime()));
          
          if (counters.containsKey(stepStats)) {
            for (Counter c : counters.get(stepStats)) {
              NestedCounter nc = new NestedCounter(c, stepStats.getName());
              nc.addParentEvent(getSimpleCheckpointToken());
              nestedCounters.add(nc);
            }
          }
        }
      }
      timer.stop();
    }
  }
}
