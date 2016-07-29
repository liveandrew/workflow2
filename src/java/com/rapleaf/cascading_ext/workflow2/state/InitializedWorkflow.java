//package com.rapleaf.cascading_ext.workflow2.state;
//
//import com.liveramp.cascading_ext.resource.ResourceManager;
//import com.liveramp.commons.util.MultiShutdownHook;
//import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
//import com.liveramp.workflow_state.InitializedPersistence;
//import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
//
//public class InitializedWorkflow<INITIALIZED extends InitializedPersistence> extends InitializedWorkflow<INITIALIZED, WorkflowOptions> {
//
//  protected InitializedWorkflow(String workflowName,
//                                WorkflowOptions options,
//                                INITIALIZED reservedPersistence,
//                                WorkflowPersistenceFactory<INITIALIZED, WorkflowOptions> factory,
//                                ResourceManager manager,
//                                MultiShutdownHook hook) {
//    super(workflowName, options, reservedPersistence, factory, manager, hook);
//  }
//}
