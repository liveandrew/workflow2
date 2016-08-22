package com.liveramp.workflow_state.runner;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;

import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.liveramp.workflow_core.runner.BaseStep;
import com.rapleaf.cascading_ext.workflow2.BaseWorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.state.BaseDbPersistenceFactory;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;

public class WorkflowRunners {

  public static BaseWorkflowRunner<Void> baseRlDbWorkflowRunner(Class name, BaseWorkflowOptions options, BaseStep step) throws IOException {
    return new BaseWorkflowRunner(new BaseDbPersistenceFactory().initialize(name.getName(), options), Sets.newHashSet(step), null);
  }

  public static BaseWorkflowRunner<Void> baseRlDbWorkflowRunner(BaseWorkflowOptions options, BaseStep step) throws IOException {
    return new BaseWorkflowRunner(new BaseDbPersistenceFactory().initialize(options), Sets.newHashSet(step), null);
  }

  public static BaseWorkflowRunner<Void> baseRldbWorkflowRunner(InitializedWorkflow workflow, Set<BaseStep<Void>> steps) throws IOException {
    return new BaseWorkflowRunner<>(workflow, steps, null);
  }

}
