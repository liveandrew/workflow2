package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.WriteResource;
import com.liveramp.commons.collections.nested_map.TwoNestedCountingMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.java_support.event_timer.MultiTimedEvent;
import com.liveramp.workflow_state.DSAction;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;

public class MultiStepAction extends Action {

  private Collection<Step> steps;

  private class MultiStepActionTimer extends MultiTimedEvent {

    public MultiStepActionTimer() {
      super(MultiStepAction.this.getActionId());
    }
  }

  public MultiStepAction(String checkpointToken, String tmpRoot) {
    this(checkpointToken, tmpRoot, null);
  }

  public MultiStepAction(String checkpointToken, String tmpRoot, Collection<Step> steps) {
    super(checkpointToken, tmpRoot);
    setSubSteps(steps);
  }

  /**
   * This method will never actually get called, since MultiStepAction is just a
   * placeholder for the workflow planner to expand later.
   */
  @Override
  public final void execute() {
    throw new IllegalStateException("planner error: method should never be called!");
  }

  protected final void setSubSteps(Collection<Step> steps) {
    Set<String> tokens = new HashSet<String>();
    if (steps == null) {
      return;
    }
    for (Step s : steps) {
      if (tokens.contains(s.getSimpleCheckpointToken())) {
        throw new IllegalArgumentException("Substep checkpoint token " + s.getSimpleCheckpointToken()
            + " is used more than once in " + this);
      }
      tokens.add(s.getSimpleCheckpointToken());
    }

    //  all hell will break loose if there are no steps in the MSA, once it gets decomposed into steps (it will get spliced from the dep graph)
    //  give a placeholder so we propagate dependencies forward
    if(steps.isEmpty()){
      this.steps = Sets.newHashSet(new Step(new NoOpAction("empty-msa-placeholder")));
    }else{
      this.steps = steps;
    }

  }

  protected final void setSubStepsFromTail(Step tail) {
    setSubStepsFromTails(Collections.singleton(tail));
  }

  protected final void setSubStepsFromTails(Step... tails) {
    setSubStepsFromTails(Arrays.asList(tails));
  }

  protected final void setSubStepsFromTails(Collection<Step> tails) {
    Set<Step> steps = new HashSet<Step>(tails);
    List<Step> queue = new ArrayList<Step>(tails);
    int index = 0;
    while (index < queue.size()) {
      Step curStep = queue.get(index);
      Set<Step> deps = curStep.getDependencies();
      for (Step curDep : deps) {
        if (!steps.contains(curDep)) {
          steps.add(curDep);
          queue.add(curDep);
        }
      }
      index++;
    }
    setSubSteps(steps);
  }

  public Set<Step> getSubSteps() {
    verifyStepsAreSet();
    return new HashSet<Step>(steps);
  }

  public Set<Step> getHeadSteps() {
    verifyStepsAreSet();
    Set<Step> heads = new HashSet<Step>();
    for (Step s : steps) {
      if (s.getDependencies().isEmpty()) {
        heads.add(s);
      }
    }
    return heads;
  }

  public Set<Step> getTailSteps() {
    verifyStepsAreSet();
    Set<Step> possibleTails = new HashSet<Step>(steps);
    for (Step s : steps) {
      possibleTails.removeAll(s.getDependencies());
    }
    return possibleTails;
  }

  @Override
  protected final void readsFrom(DataStore store) {
    throw new RuntimeException("Cannot set a datastore to read from for a multistep action");
  }

  @Override
  protected final void creates(DataStore store) {
    throw new RuntimeException("Cannot set a datastore to create for a multistep action");
  }

  @Override
  protected final void createsTemporary(DataStore store) {
    throw new RuntimeException(
        "Cannot set a datastore to temporarily create for a multistep action");
  }

  @Override
  protected final void writesTo(DataStore store) {
    throw new RuntimeException("Cannot set a datastore to write to for a multistep action");
  }

  @Deprecated
  protected <T> T get(OldResource<T> resource) throws IOException {
    throw new RuntimeException("Cannot get a resource in a multistep action!");
  }

  protected <T> T get(ReadResource<T> resource) {
    throw new RuntimeException("Cannot get a resource in a multistep action!");
  }

  protected <T, R extends WriteResource<T>> void set(R resource, T value) {
    throw new RuntimeException("Cannot set a resource in a multistep action!");
  }

  @Deprecated
  protected <T> void set(OldResource<T> resource, T value) throws IOException {
    throw new RuntimeException("Cannot set a resource in a multistep action!");
  }

  private void verifyStepsAreSet() {
    if (steps == null) {
      throw new RuntimeException(
          "Steps in a multi-step action must be set before thay can be used!");
    }
  }

  @Override
  public Set<DataStore> getDatastores(DSAction... actions) {

    Set<DataStore> combined = Sets.newHashSet();

    for (Step step : getSubSteps()) {
      combined.addAll(step.getAction().getDatastores(actions));
    }

    return combined;
  }

  @Override
  public void setFailOnCounterFetch(boolean value) {
    for (Step step : getSubSteps()) {
      step.getAction().setFailOnCounterFetch(value);
    }
  }

  @Override
  TwoNestedMap<String, String, Long> getStepCounters() throws IOException {
    TwoNestedCountingMap<String, String> map = new TwoNestedCountingMap<>(0L);
    for (Step step : steps) {
      map.incrementAll(step.getAction().getStepCounters());
    }
    return map;
  }

  @Override
  DurationInfo getDurationInfo() throws IOException {

    long minStart = Long.MAX_VALUE;
    long maxEnd = Long.MIN_VALUE;

    for (Step step : steps) {
      DurationInfo durationInfo = step.getAction().getDurationInfo();
      minStart = Math.min(minStart, durationInfo.getStartTime());
      maxEnd = Math.max(maxEnd, durationInfo.getEndTime());
    }

    return new DurationInfo(minStart, maxEnd);
  }
}
