package com.liveramp.workflow.state;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_db_state.BaseWorkflowDbPersistenceFactory;
import com.liveramp.workflow_db_state.CoreWorkflowDbPersistenceFactory;
import com.liveramp.workflow_db_state.InitializedDbPersistence;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;

public class WorkflowDbPersistenceFactory extends CoreWorkflowDbPersistenceFactory<WorkflowOptions, DbHadoopWorkflow> {
  @Override
  public DbHadoopWorkflow construct(String workflowName, WorkflowOptions options, InitializedDbPersistence initializedDbPersistence, ResourceManager manager, MultiShutdownHook hook) {
    return new DbHadoopWorkflow(workflowName, options, initializedDbPersistence, this, manager, hook);
  }
}