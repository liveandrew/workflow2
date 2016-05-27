package com.rapleaf.cascading_ext.workflow2.state;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.workflow_state.InitializedPersistence;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;

public class InitializedWorkflow<INITIALIZED extends InitializedPersistence> {

  private final String workflowName;
  private final WorkflowOptions options;
  private final INITIALIZED persistence;
  private final WorkflowPersistenceFactory<INITIALIZED> factory;
  private final MultiShutdownHook hook;

  protected InitializedWorkflow(String workflowName,
                                WorkflowOptions options,
                                INITIALIZED reservedPersistence,
                                WorkflowPersistenceFactory<INITIALIZED> factory,
                                MultiShutdownHook hook){

    this.workflowName = workflowName;
    this.options = options;
    this.persistence = reservedPersistence;
    this.factory = factory;
    this.hook = hook;

  }

  public String getWorkflowName() {
    return workflowName;
  }

  public WorkflowOptions getOptions() {
    return options;
  }

  public INITIALIZED getInitializedPersistence() {
    return persistence;
  }

  public WorkflowStatePersistence prepare(DirectedGraph<Step, DefaultEdge> steps){
    return factory.prepare(persistence, steps);
  }

  public MultiShutdownHook getShutdownHook() {
    return hook;
  }
}
