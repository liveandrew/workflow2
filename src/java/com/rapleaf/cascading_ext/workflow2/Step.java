package com.rapleaf.cascading_ext.workflow2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.support.event_timer.EventTimer;
import com.rapleaf.support.event_timer.TimedEvent;

public final class Step {

  private String checkpointTokenPrefix = "";
  private final Action action;
  private final Set<Step> dependencies;
  private Set<Step> children;
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

  public Step(Action action, Step... dependencies) {
    this(action, Arrays.asList(dependencies));
  }

  public Step(Action action, List<Step> dependencies) {
    this.action = action;
    children = new HashSet<Step>();
    this.dependencies = new HashSet<Step>(dependencies);
    if (this.dependencies.contains(null)) {
      throw new NullPointerException("null cannot be a dependency for a step!");
    }
    for (Step dependent : this.dependencies) {
      dependent.addChild(this);
    }
  }

  private void addChild(Step child) {
    if (!child.getDependencies().contains(this)) {
      throw new RuntimeException("child (" + child + ") does not depend on this (" + this + ")");
    }
    children.add(child);
  }

  public Set<Step> getChildren() {
    return Collections.unmodifiableSet(children);
  }

  public Set<Step> getDependencies() {
    return Collections.unmodifiableSet(dependencies);
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

  public TimedEvent getTimer() {
    if (action instanceof MultiStepAction) {
      return ((MultiStepAction)action).getMultiStepActionTimer();

    } else {
      return timer;
    }
  }

  public List<NestedCounter> getCounters() {
    return nestedCounters;
  }

  public void run() {
    timer.start();
    try {
      action.internalExecute();
    } finally {
      for (ActionOperation operation : action.getRunFlows()) {
        operation.timeOperation(timer, getCheckpointToken(), nestedCounters);
      }

      timer.stop();
    }
  }
}
