package com.liveramp.workflow_db_state;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_core.CoreWorkflowOptions;
import com.liveramp.workflow_core.JVMState;
import com.liveramp.workflow_core.runner.BaseStep;

public class BaseWorkflowDbPersistenceFactory extends CoreWorkflowDbPersistenceFactory<BaseStep, CoreWorkflowOptions, DbWorkflow> {
  public BaseWorkflowDbPersistenceFactory() {
    super(new JVMState());
  }

  @Override
  public DbWorkflow construct(String workflowName, CoreWorkflowOptions options, InitializedDbPersistence initializedDbPersistence, ResourceManager manager, MultiShutdownHook hook) {
    return new DbWorkflow(workflowName, options, initializedDbPersistence, this, manager, hook);
  }
}
