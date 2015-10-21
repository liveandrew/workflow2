package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import com.liveramp.cascading_ext.util.HadoopProperties;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;

public final class Step {

  private final Action action;
  private final Set<Step> dependencies;
  private Set<Step> children;

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
    return getAction().fullId();
  }

  public String getSimpleCheckpointToken() {
    return getAction().getActionId().getRelativeName();
  }

  @Override
  public String toString() {
    return "Step " + getCheckpointToken() + " " + action + " deps=" + dependencies;
  }

  public Callable<TwoNestedMap<String, String, Long>> getCountersFuture() throws IOException {
    return new Callable<TwoNestedMap<String, String, Long>>() {
      @Override
      public TwoNestedMap<String, String, Long> call() throws Exception {
        return action.getStepCounters();
      }
    };
  }

  public void run(HadoopProperties properties) {
    action.internalExecute(properties);
  }
}
