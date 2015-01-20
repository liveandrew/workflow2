package com.rapleaf.cascading_ext.workflow2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.liveramp.cascading_ext.event_timer.EventTimer;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.stats.StepStatsRecorder;
import com.rapleaf.support.event_timer.TimedEvent;

public final class Step {

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

  public Step(Action action, Collection<Step> dependencies) {
    this.action = action;
    children = new HashSet<Step>();
    this.dependencies = new HashSet<Step>(dependencies);
    if (this.dependencies.contains(null)) {
      throw new NullPointerException("null cannot be a dependency for a step!");
    }
    for (Step dependency : this.dependencies) {
      dependency.addChild(this);
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

  public String getCheckpointToken() {
    return getAction().getActionId().resolve();
  }

  public String getSimpleCheckpointToken() {
    return getAction().getActionId().getRelativeName();
  }

  @Override
  public String toString() {
    return "Step " + getCheckpointToken() + " " + action + " deps=" + dependencies;
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

  public void run(WorkflowStatePersistence persistence, ContextStorage storage, List<StepStatsRecorder> recorders, Map<Object, Object> properties) {

    timer.start();
    try {
      action.internalExecute(persistence, storage, properties);
    } finally {
      for (ActionOperation operation : action.getRunFlows()) {
        operation.timeOperation(timer, getCheckpointToken(), nestedCounters);
      }

      timer.stop();

      for (StepStatsRecorder recorder : recorders) {
        recorder.recordStats(this, timer);
      }
    }
  }
}
