package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.datastore.DataStore;

import java.util.*;

public class MultiStepAction extends Action {

  private Collection<Step> steps;
  private final MultiStepActionTimer timer = new MultiStepActionTimer();

  private class MultiStepActionTimer extends MultiTimedEvent {

    public MultiStepActionTimer() {
      super(MultiStepAction.this.getCheckpointToken());
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

  protected void setSubSteps(Collection<Step> steps) {
    Set<String> tokens = new HashSet<String>();
    if (steps == null) {
      return;
    }
    for (Step s : steps) {
      if (tokens.contains(s.getCheckpointToken())) {
        throw new IllegalArgumentException("Substep checkpoint token " + s.getCheckpointToken()
            + " is used more than once in " + this);
      }
      tokens.add(s.getCheckpointToken());
      timer.addChild(s.getTimer());
    }
    this.steps = steps;
  }

  protected void setSubStepsFromTail(Step tail) {
    setSubStepsFromTails(Collections.singleton(tail));
  }

  protected void setSubStepsFromTails(Collection<Step> tails) {
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
  public Set<DataStore> getReadsFromDatastores() {
    Set<DataStore> datastores = new HashSet<DataStore>();
    for (Step step : getHeadSteps()) {
      datastores.addAll(step.getAction().getReadsFromDatastores());
    }
    return datastores;
  }

  @Override
  public Set<DataStore> getCreatesDatastores() {
    Set<DataStore> datastores = new HashSet<DataStore>();
    for (Step step : getSubSteps()) {
      datastores.addAll(step.getAction().getCreatesDatastores());
    }
    return datastores;
  }

  @Override
  public Set<DataStore> getWritesToDatastores() {
    Set<DataStore> datastores = new HashSet<DataStore>();
    for (Step step : getSubSteps()) {
      datastores.addAll(step.getAction().getWritesToDatastores());
    }
    return datastores;
  }

  public MultiStepActionTimer getMultiStepActionTimer() {
    return timer;
  }
}
