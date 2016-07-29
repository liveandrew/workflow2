package com.liveramp.workflow_core.runner;

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
import com.liveramp.workflow_core.OldResource;
import com.liveramp.workflow_core.step.NoOp;

public class BaseMultiStepAction<Config> extends BaseAction<Config> {

  private Collection<? extends BaseStep<Config>> steps;

  public BaseMultiStepAction(String checkpointToken, Collection<? extends BaseStep<Config>> steps) {
    super(checkpointToken);
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

  public final void setSubSteps(Collection<? extends BaseStep<Config>> steps) {
    Set<String> tokens = new HashSet<>();
    if (steps == null) {
      return;
    }
    for (BaseStep<Config> s : steps) {
      if (tokens.contains(s.getSimpleCheckpointToken())) {
        throw new IllegalArgumentException("Substep checkpoint token " + s.getSimpleCheckpointToken()
            + " is used more than once in " + this);
      }
      tokens.add(s.getSimpleCheckpointToken());
    }

    //  all hell will break loose if there are no steps in the MSA, once it gets decomposed into steps (it will get spliced from the dep graph)
    //  give a placeholder so we propagate dependencies forward
    if(steps.isEmpty()){
      this.steps = Sets.newHashSet(new BaseStep<Config>(new NoOp("empty-msa-placeholder")));
    }else{
      this.steps = steps;
    }

  }

  protected final void setSubStepsFromTail(BaseStep<Config> tail) {
    setSubStepsFromTails(Collections.singleton(tail));
  }

  protected final void setSubStepsFromTails(BaseStep<Config>... tails) {
    setSubStepsFromTails(Arrays.asList(tails));
  }

  protected final void setSubStepsFromTails(Collection<? extends BaseStep<Config>> tails) {
    Set<BaseStep<Config>> steps = new HashSet<>(tails);
    List<BaseStep<Config>> queue = new ArrayList<>(tails);
    int index = 0;
    while (index < queue.size()) {
      BaseStep<Config> curStep = queue.get(index);
      Set<BaseStep<Config>> deps = curStep.getDependencies();
      for (BaseStep<Config> curDep : deps) {
        if (!steps.contains(curDep)) {
          steps.add(curDep);
          queue.add(curDep);
        }
      }
      index++;
    }
    setSubSteps(steps);
  }

  public Set<BaseStep<Config>> getSubSteps() {
    verifyStepsAreSet();
    return new HashSet<>(steps);
  }

  public Set<BaseStep<Config>> getHeadSteps() {
    verifyStepsAreSet();
    Set<BaseStep<Config>> heads = new HashSet<>();
    for (BaseStep<Config> s : steps) {
      if (s.getDependencies().isEmpty()) {
        heads.add(s);
      }
    }
    return heads;
  }

  public Set<BaseStep<Config>> getTailSteps() {
    verifyStepsAreSet();
    Set<BaseStep<Config>> possibleTails = new HashSet<>(steps);
    for (BaseStep<Config> s : steps) {
      possibleTails.removeAll(s.getDependencies());
    }
    return possibleTails;
  }

  private void verifyStepsAreSet() {
    if (steps == null) {
      throw new RuntimeException(
          "Steps in a multi-step action must be set before thay can be used!");
    }
  }

  @Override
  public void setFailOnCounterFetch(boolean value) {
    for (BaseStep<Config> step : getSubSteps()) {
      step.getAction().setFailOnCounterFetch(value);
    }
  }

  @Override
  public TwoNestedMap<String, String, Long> getStepCounters() throws IOException {
    TwoNestedCountingMap<String, String> map = new TwoNestedCountingMap<>(0L);
    for (BaseStep<Config> step : steps) {
      map.incrementAll(step.getAction().getStepCounters());
    }
    return map;
  }

  @Override
  public BaseAction.DurationInfo getDurationInfo() throws IOException {

    long minStart = Long.MAX_VALUE;
    long maxEnd = Long.MIN_VALUE;

    for (BaseStep<Config> step : steps) {
      BaseAction.DurationInfo durationInfo = step.getAction().getDurationInfo();
      minStart = Math.min(minStart, durationInfo.getStartTime());
      maxEnd = Math.max(maxEnd, durationInfo.getEndTime());
    }

    return new BaseAction.DurationInfo(minStart, maxEnd);
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

}
