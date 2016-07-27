package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.DataStoreInfo;
import com.liveramp.workflow_state.IStep;

public final class Step implements IStep {

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

  @Override
  public Multimap<DSAction, DataStoreInfo> getDataStores() {
    return action.getAllDataStoreInfo();
  }

  public String getCheckpointToken() {
    return getAction().fullId();
  }

  @Override
  public String getActionClass() {
    return getAction().getClass().getName();
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

  public Callable<Action.DurationInfo> getDurationFuture(){
    return new Callable<Action.DurationInfo>() {
      @Override
      public Action.DurationInfo call() throws Exception {
        return action.getDurationInfo();
      }
    };
  }

  public void run(OverridableProperties properties) {
    action.internalExecute(properties);
  }
}
