package com.rapleaf.cascading_ext.workflow2;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class Step {
  private String checkpointTokenPrefix = "";
  private final Action action;
  private final Set<Step> dependencies;
  
  public Step(Action action) {
    this.action = action;
    dependencies = Collections.EMPTY_SET;
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
  
  void run() {
    action.internalExecute();
  }
}
