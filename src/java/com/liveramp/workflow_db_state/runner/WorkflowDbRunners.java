package com.liveramp.workflow_db_state.runner;

import com.google.common.collect.Sets;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.liveramp.workflow_core.runner.BaseStep;
import com.liveramp.workflow_db_state.BaseWorkflowDbPersistenceFactory;
import com.rapleaf.cascading_ext.workflow2.BaseWorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;

import java.io.IOException;
import java.util.Set;

public class WorkflowDbRunners {

  public static BaseWorkflowRunner<Void> baseWorkflowDbRunner(Class name, BaseWorkflowOptions options, BaseStep step) throws IOException {
    return new BaseWorkflowRunner(new BaseWorkflowDbPersistenceFactory().initialize(name.getName(), options), Sets.newHashSet(step), null);
  }

  public static BaseWorkflowRunner<Void> baseWorkflowDbRunner(BaseWorkflowOptions options, BaseStep step) throws IOException {
    return new BaseWorkflowRunner(new BaseWorkflowDbPersistenceFactory().initialize(options), Sets.newHashSet(step), null);
  }

  public static BaseWorkflowRunner<Void> baseWorkflowDbRunner(InitializedWorkflow workflow, Set<BaseStep<Void>> steps) throws IOException {
    return new BaseWorkflowRunner<>(workflow, steps, null);
  }

}
