package com.rapleaf.cascading_ext.workflow2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.support.Rap;
import com.rapleaf.support.event_timer.EventTimer;
import com.rapleaf.support.event_timer.TimedEvent;

public final class Step {

  private String checkpointTokenPrefix = "";
  private final Action action;
  private final Set<Step> dependencies;
  private Set<Step> children;
  private final StepTimer timer = new StepTimer();
  private final List<NestedCounter> nestedCounters = new ArrayList<NestedCounter>();
  private final Set<Conditional> conditions = Sets.newHashSet();

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
    this(action, dependencies, Lists.<Conditional>newArrayList());
  }

  public Step(Action action, Collection<Step> dependencies, Collection<Conditional> conditions) {
    this.action = action;
    children = new HashSet<Step>();
    this.dependencies = new HashSet<Step>(dependencies);
    if (this.dependencies.contains(null)) {
      throw new NullPointerException("null cannot be a dependency for a step!");
    }
    for (Step dependency : this.dependencies) {
      dependency.addChild(this);
    }
    this.conditions.addAll(conditions);
    if (action instanceof MultiStepAction) {
      ((MultiStepAction) action).setConditionsOnSubSteps(conditions);
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
      return ((MultiStepAction) action).getMultiStepActionTimer();
    } else {
      return timer;
    }
  }

  public void addCondition(Conditional condition) {
    addConditions(Sets.newHashSet(condition));
  }

  public void addConditions(Collection<Conditional> conditions) {
    this.conditions.addAll(conditions);
    if (getAction() instanceof MultiStepAction) {
      ((MultiStepAction) getAction()).setConditionsOnSubSteps(conditions);
    }
  }

  public Set<Conditional> getConditions() {
    return conditions;
  }

  public List<NestedCounter> getCounters() {
    return nestedCounters;
  }

  @Deprecated
  //  if you are calling this method, you are wrong
  public void run() {
    this.run(Lists.<StepStatsRecorder>newArrayList(), Maps.newHashMap());
  }

  public void run(List<StepStatsRecorder> recorders, Map<Object, Object> properties) {
    if (conditionsNotMet()) {
      return;
    }
    timer.start();
    try {
      action.internalExecute(properties);
    } finally {
      for (ActionOperation operation : action.getRunFlows()) {
        operation.timeOperation(timer, getCheckpointToken(), nestedCounters);
      }

      timer.stop();

      if (!Rap.getTestMode()) {
        for (StepStatsRecorder recorder : recorders) {
          recorder.recordStats(this, timer);
        }
      }
    }
  }

  private boolean conditionsNotMet() {
    for (Conditional condition : conditions) {
      if (!condition.shouldRun()) {
        return true;
      }
    }
    return false;
  }

  public interface Conditional {
    public boolean shouldRun();
  }
}
