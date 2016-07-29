package com.rapleaf.cascading_ext.workflow2.state;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.liveramp.workflow_state.IStep;
import com.liveramp.workflow_state.InitializedPersistence;
import com.liveramp.workflow_state.WorkflowStatePersistence;

public class InitializedWorkflow<INITIALIZED extends InitializedPersistence, OPTS extends BaseWorkflowOptions> {


  private final String workflowName;
  private final OPTS options;
  private final INITIALIZED persistence;
  private final WorkflowPersistenceFactory<INITIALIZED, OPTS> factory;
  private final ResourceManager manager;
  private final MultiShutdownHook hook;

  public InitializedWorkflow(String workflowName,
                             OPTS options,
                             INITIALIZED reservedPersistence,
                             WorkflowPersistenceFactory<INITIALIZED, OPTS> factory,
                             ResourceManager manager,
                             MultiShutdownHook hook){

    this.workflowName = workflowName;
    this.options = options;
    this.persistence = reservedPersistence;
    this.factory = factory;
    this.manager = manager;
    this.hook = hook;

  }

  public ResourceManager getManager() {
    return manager;
  }

  public String getWorkflowName() {
    return workflowName;
  }

  public OPTS getOptions() {
    return options;
  }

  public INITIALIZED getInitializedPersistence() {
    return persistence;
  }

  public WorkflowStatePersistence prepare(DirectedGraph<IStep, DefaultEdge> steps){
    return factory.prepare(persistence, steps);
  }

  public MultiShutdownHook getShutdownHook() {
    return hook;
  }
}
