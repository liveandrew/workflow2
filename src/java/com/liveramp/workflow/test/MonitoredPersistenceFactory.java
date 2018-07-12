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
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow.state.DbHadoopWorkflow;
import com.liveramp.workflow_db_state.InitializedDbPersistence;
import com.liveramp.workflow_state.IStep;
import com.liveramp.workflow_state.InitializedPersistence;
import com.liveramp.workflow.types.StepStatus;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public class MonitoredPersistenceFactory extends WorkflowPersistenceFactory<InitializedDbPersistence, WorkflowOptions, DbHadoopWorkflow> {

  private final WorkflowPersistenceFactory<InitializedDbPersistence, WorkflowOptions, DbHadoopWorkflow> delegate;

  public MonitoredPersistenceFactory(WorkflowPersistenceFactory<InitializedDbPersistence, WorkflowOptions, DbHadoopWorkflow> delegate) {
    this.delegate = delegate;
  }

  @Override
  public DbHadoopWorkflow construct(String workflowName, WorkflowOptions options, InitializedDbPersistence initialized, ResourceManager manager, MultiShutdownHook hook) {
    return new DbHadoopWorkflow(workflowName, options, initialized, this, manager, hook);
  }

  @Override
  public InitializedDbPersistence initializeInternal(String name, String scopeId, String description, AppType appType, String host, String username, String pool, String priority, String launchDir, String launchJar, Set<WorkflowRunnerNotification> configuredNotifications, AlertsHandler providedHandler, Class<? extends ResourceDeclarerFactory> resourceFactory, String remote, String implementationBuild) throws IOException {
    return delegate.initializeInternal(name, scopeId, description, appType, host, username, pool, priority, launchDir, launchJar, configuredNotifications, providedHandler, resourceFactory, remote, implementationBuild);
  }

  @Override
  public <S extends IStep> MonitoredPersistence prepare(InitializedDbPersistence initialized, DirectedGraph<S, DefaultEdge> flatSteps) {
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
