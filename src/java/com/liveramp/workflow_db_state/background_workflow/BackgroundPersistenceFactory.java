package com.liveramp.workflow_db_state.background_workflow;

import java.io.Serializable;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_core.BackgroundState;
import com.liveramp.workflow_core.CoreWorkflowOptions;
import com.liveramp.workflow_core.StepStateManager;
import com.liveramp.workflow_core.background_workflow.BackgroundStep;
import com.liveramp.workflow_core.runner.BaseStep;
import com.liveramp.workflow_db_state.CoreWorkflowDbPersistenceFactory;
import com.liveramp.workflow_db_state.DbWorkflow;
import com.liveramp.workflow_db_state.InitializedDbPersistence;

public class BackgroundPersistenceFactory extends CoreWorkflowDbPersistenceFactory<BackgroundStep, CoreWorkflowOptions, BackgroundWorkflow> {

  public BackgroundPersistenceFactory() {
    super(new BackgroundState());
  }

  @Override
  public BackgroundWorkflow construct(String workflowName, CoreWorkflowOptions options,
                                      InitializedDbPersistence initializedDbPersistence,
                                      ResourceManager manager, MultiShutdownHook hook) {
    return new BackgroundWorkflow(workflowName, options, initializedDbPersistence, this, manager, hook);
  }
}
