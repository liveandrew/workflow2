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
import com.liveramp.workflow_core.step.NoOp;
import com.rapleaf.cascading_ext.workflow2.Step;

public class BaseMultiStepAction<Config> extends BaseAction<Config> {

  private Collection<Step> steps;

  public BaseMultiStepAction(String checkpointToken, Collection<Step> steps) {
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

  public final void setSubSteps(Collection<Step> steps) {
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
      this.steps = Sets.newHashSet(new Step(new NoOp("empty-msa-placeholder")));
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

  protected <T> T get(ReadResource<T> resource) {
    throw new RuntimeException("Cannot get a resource in a multistep action!");
  }

  protected <T, R extends WriteResource<T>> void set(R resource, T value) {
    throw new RuntimeException("Cannot set a resource in a multistep action!");
  }

  private void verifyStepsAreSet() {
    if (steps == null) {
      throw new RuntimeException(
          "Steps in a multi-step action must be set before thay can be used!");
    }
  }

  @Override
  public void setFailOnCounterFetch(boolean value) {
    for (Step step : getSubSteps()) {
      step.getAction().setFailOnCounterFetch(value);
    }
  }

  @Override
  public TwoNestedMap<String, String, Long> getStepCounters() throws IOException {
    TwoNestedCountingMap<String, String> map = new TwoNestedCountingMap<>(0L);
    for (Step step : steps) {
      map.incrementAll(step.getAction().getStepCounters());
    }
    return map;
  }

  @Override
  public BaseAction.DurationInfo getDurationInfo() throws IOException {

    long minStart = Long.MAX_VALUE;
    long maxEnd = Long.MIN_VALUE;

    for (Step step : steps) {
      BaseAction.DurationInfo durationInfo = step.getAction().getDurationInfo();
      minStart = Math.min(minStart, durationInfo.getStartTime());
      maxEnd = Math.max(maxEnd, durationInfo.getEndTime());
    }

    return new BaseAction.DurationInfo(minStart, maxEnd);
  }
}
