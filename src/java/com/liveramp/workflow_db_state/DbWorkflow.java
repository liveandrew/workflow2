package com.liveramp.workflow_db_state;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_core.CoreWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public class DbWorkflow extends InitializedWorkflow<InitializedDbPersistence, CoreWorkflowOptions>{
  public DbWorkflow(String workflowName, CoreWorkflowOptions options, InitializedDbPersistence reservedPersistence, WorkflowPersistenceFactory<InitializedDbPersistence, CoreWorkflowOptions, ?> factory, ResourceManager manager, MultiShutdownHook hook) {
    super(workflowName, options, reservedPersistence, factory, manager, hook);
  }
}
