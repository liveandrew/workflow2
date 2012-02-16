package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.Flow;
import cascading.stats.StepStats;
import com.rapleaf.support.event_timer.EventTimer;
import com.rapleaf.support.event_timer.FixedTimedEvent;

import java.util.*;

public final class Step {
  
  private String checkpointTokenPrefix = "";
  private final String checkpointToken;
  private final Action action;
  private final Set<Step> dependencies;
  private final StepTimer timer = new StepTimer();
  
  public class StepTimer extends EventTimer {
    
    public StepTimer() {
      super(null);
    }
    
    @Override
    public String getEventName() {
      return getSimpleCheckpointToken();
    }
  }
  
  public Step(String checkpointToken, Action action) {
    this.checkpointToken = checkpointToken;
    this.action = action;
    dependencies = Collections.EMPTY_SET;
  }
  
  public Step(String checkpointToken, Action action, Step previous, Step... rest) {
    this.checkpointToken = checkpointToken;
    this.action = action;
    dependencies = new HashSet<Step>(Arrays.asList(rest));
    dependencies.add(previous);
    if (dependencies.contains(null)) {
      throw new NullPointerException("null cannot be a dependency for a step!");
    }
  }
  
  public Step(String checkpointToken, Action action, List<Step> steps) {
    this.checkpointToken = checkpointToken;
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
    return getCheckpointTokenPrefix() + checkpointToken;
  }
  
  public String getSimpleCheckpointToken() {
    return checkpointToken;
  }
  
  @Override
  public String toString() {
    return "Step " + checkpointToken + " " + action + " deps=" + dependencies;
  }

  public StepTimer getTimer() {
    return timer;
  }
  
  void run() {
    timer.start();
    try {
      action.internalExecute();
    } finally {
      // If needed, add timers for flow steps that have been executed by the action
      for (Flow flow : action.getRunFlows()) {
        for (StepStats stepStats : flow.getFlowStats().getStepStats()) {
          timer.addChild(new FixedTimedEvent(stepStats.getName(), stepStats.getStartTime(), stepStats.getFinishedTime()));
        }
      }
      timer.stop();
    }
  }
}
