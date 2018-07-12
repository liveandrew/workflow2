package com.rapleaf.cascading_ext.workflow2.state;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_db_state.InitializedDbPersistence;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;

public class HadoopWorkflow extends InitializedWorkflow<HdfsInitializedPersistence, WorkflowOptions> {
  public HadoopWorkflow(String workflowName, WorkflowOptions options, HdfsInitializedPersistence reservedPersistence, WorkflowPersistenceFactory<HdfsInitializedPersistence, WorkflowOptions, ?> factory, ResourceManager manager, MultiShutdownHook hook) {
    super(workflowName, options, reservedPersistence, factory, manager, hook);
  }
}
