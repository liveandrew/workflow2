package com.liveramp.workflow_db_state;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_core.CoreWorkflowOptions;

public class BaseWorkflowDbPersistenceFactory extends CoreWorkflowDbPersistenceFactory<CoreWorkflowOptions, DbWorkflow>{

  @Override
  public DbWorkflow construct(String workflowName, CoreWorkflowOptions options, InitializedDbPersistence initializedDbPersistence, ResourceManager manager, MultiShutdownHook hook) {
    return new DbWorkflow(workflowName, options, initializedDbPersistence, this, manager, hook);
  }
}
