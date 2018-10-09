package com.liveramp.workflow_db_state.background_workflow;

import java.io.Serializable;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_core.CoreWorkflowOptions;
import com.liveramp.workflow_core.background_workflow.BackgroundStep;
import com.liveramp.workflow_db_state.InitializedDbPersistence;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public class BackgroundWorkflow extends InitializedWorkflow<
    BackgroundStep,
    InitializedDbPersistence,
    CoreWorkflowOptions> {

  public BackgroundWorkflow(String workflowName, CoreWorkflowOptions options, InitializedDbPersistence reservedPersistence,
                            WorkflowPersistenceFactory<BackgroundStep, InitializedDbPersistence, CoreWorkflowOptions, ?> factory,
                            ResourceManager manager, MultiShutdownHook hook) {
    super(workflowName, options, reservedPersistence, factory, manager, hook);
  }

}
