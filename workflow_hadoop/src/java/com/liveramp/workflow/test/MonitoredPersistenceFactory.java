package com.liveramp.workflow.test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.cascading_ext.resource.ResourceDeclarerFactory;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_core.JVMState;
import com.liveramp.workflow.state.DbHadoopWorkflow;
import com.liveramp.workflow_db_state.InitializedDbPersistence;
import com.liveramp.workflow.types.StepStatus;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public class MonitoredPersistenceFactory extends WorkflowPersistenceFactory<Step, InitializedDbPersistence, HadoopWorkflowOptions, DbHadoopWorkflow> {

  private final WorkflowPersistenceFactory<Step, InitializedDbPersistence, HadoopWorkflowOptions, DbHadoopWorkflow> delegate;

  public MonitoredPersistenceFactory(WorkflowPersistenceFactory<Step, InitializedDbPersistence, HadoopWorkflowOptions, DbHadoopWorkflow> delegate) {
    super(new JVMState());
    this.delegate = delegate;
  }

  @Override
  public DbHadoopWorkflow construct(String workflowName, HadoopWorkflowOptions options, InitializedDbPersistence initialized, ResourceManager manager, MultiShutdownHook hook) {
    return new DbHadoopWorkflow(workflowName, options, initialized, this, manager, hook);
  }

  @Override
  public InitializedDbPersistence initializeInternal(String name, String scopeId, String description, Integer appType, String host, String username, String pool, String priority, String launchDir, String launchJar, Set<WorkflowRunnerNotification> configuredNotifications, AlertsHandler providedHandler, Class<? extends ResourceDeclarerFactory> resourceFactory, String remote, String implementationBuild) throws IOException {
    return delegate.initializeInternal(name, scopeId, description, appType, host, username, pool, priority, launchDir, launchJar, configuredNotifications, providedHandler, resourceFactory, remote, implementationBuild);
  }

  @Override
  public  MonitoredPersistence prepare(InitializedDbPersistence initialized, DirectedGraph<Step, DefaultEdge> flatSteps) {
    return new MonitoredPersistence(delegate.prepare(initialized, flatSteps));
  }

  public static class MonitoredPersistence extends ForwardingPersistence{

    private AtomicInteger getStepStatusCalls = new AtomicInteger(0);

    public MonitoredPersistence(WorkflowStatePersistence delegate) {
      super(delegate);
    }

    @Override
    public Map<String, StepStatus> getStepStatuses() throws IOException {
      getStepStatusCalls.incrementAndGet();
      return delegatePersistence.getStepStatuses();
    }

    public int getGetStepStatusCalls() {
      return getStepStatusCalls.get();
    }
  }
}
