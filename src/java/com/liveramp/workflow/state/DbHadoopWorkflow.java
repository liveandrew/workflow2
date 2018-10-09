package com.liveramp.workflow.state;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.liveramp.workflow_core.runner.BaseStep;
import com.liveramp.workflow_db_state.InitializedDbPersistence;
import com.liveramp.workflow_state.InitializedPersistence;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public class DbHadoopWorkflow extends InitializedWorkflow<Step, InitializedDbPersistence, WorkflowOptions> {
  public DbHadoopWorkflow(String workflowName, WorkflowOptions options, InitializedDbPersistence reservedPersistence, WorkflowPersistenceFactory<Step, InitializedDbPersistence, WorkflowOptions, ?> factory, ResourceManager manager, MultiShutdownHook hook) {
    super(workflowName, options, reservedPersistence, factory, manager, hook);
  }
}
