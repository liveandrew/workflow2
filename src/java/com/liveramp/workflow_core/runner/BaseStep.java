package com.liveramp.workflow_core.runner;

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

public class BaseStep<Config> implements IStep {

  private final BaseAction<Config> action;
  private final Set<BaseStep<Config>> dependencies;
  private Set<BaseStep<Config>> children;

  public BaseStep(BaseAction<Config> action, BaseStep<Config>... dependencies) {
    this(action, Arrays.asList(dependencies));
  }

  public BaseStep(BaseAction<Config> action, Collection<? extends BaseStep<Config>> dependencies) {
    this.action = action;
    children = new HashSet<BaseStep<Config>>();
    this.dependencies = new HashSet<>(dependencies);
    if (this.dependencies.contains(null)) {
      throw new NullPointerException("null cannot be a dependency for a step!");
    }
    for (BaseStep dependency : this.dependencies) {
      dependency.addChild(this);
    }
  }

  private void addChild(BaseStep child) {
    if (!child.getDependencies().contains(this)) {
      throw new RuntimeException("child (" + child + ") does not depend on this (" + this + ")");
    }
    children.add(child);
  }

  public Set<BaseStep<Config>> getChildren() {
    return Collections.unmodifiableSet(children);
  }

  public Set<BaseStep<Config>> getDependencies() {
    return Collections.unmodifiableSet(dependencies);
  }

  public BaseAction getAction() {
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

  public Callable<BaseAction.DurationInfo> getDurationFuture(){
    return new Callable<BaseAction.DurationInfo>() {
      @Override
      public BaseAction.DurationInfo call() throws Exception {
        return action.getDurationInfo();
      }
    };
  }

  public void run(OverridableProperties properties) {
    action.internalExecute(properties);
  }

}
