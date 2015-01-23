package com.rapleaf.cascading_ext.workflow2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import com.liveramp.java_support.event_timer.MultiTimedEvent;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.db_schemas.rldb.workflow.DSAction;

public class MultiStepAction extends Action {

  private Collection<Step> steps;
  private final MultiStepActionTimer timer = new MultiStepActionTimer();

  private class MultiStepActionTimer extends MultiTimedEvent {

    public MultiStepActionTimer() {
      super(MultiStepAction.this.getActionId());
    }
  }

  public MultiStepAction(String checkpointToken) {
    this(checkpointToken, null, null);
  }

  public MultiStepAction(String checkpointToken, Collection<Step> steps) {
    this(checkpointToken, null, steps);
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
        throw new IllegalArgumentException("Substep checkpoint token " + s.getCheckpointToken()
            + " is used more than once in " + this);
      }
      tokens.add(s.getSimpleCheckpointToken());
      timer.addChild(s.getTimer());
    }
    this.steps = steps;
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

  public MultiStepActionTimer getMultiStepActionTimer() {
    return timer;
  }

  public List<NestedCounter> getCounters() {
    // we don't know what stage of execution we are in when this is called
    // so get an up-to-date list of counters each time
    List<NestedCounter> counters = new ArrayList<NestedCounter>();
    for (Step s : steps) {
      for (NestedCounter c : s.getCounters()) {
        counters.add(c.addParentEvent(s.getCheckpointToken()));
      }
    }
    return counters;
  }
}
