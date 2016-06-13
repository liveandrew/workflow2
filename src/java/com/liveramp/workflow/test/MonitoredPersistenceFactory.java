package com.liveramp.workflow.test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_state.IStep;
import com.liveramp.workflow_state.InitializedPersistence;
import com.liveramp.workflow_state.StepStatus;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public class MonitoredPersistenceFactory<INITIALIZED extends InitializedPersistence> extends WorkflowPersistenceFactory<INITIALIZED> {

  private final WorkflowPersistenceFactory<INITIALIZED> delegate;

  public MonitoredPersistenceFactory(WorkflowPersistenceFactory<INITIALIZED> delegate) {
    this.delegate = delegate;
  }

  @Override
  public INITIALIZED initializeInternal(String name, String scopeId, String description, AppType appType, String host, String username, String pool, String priority, String launchDir, String launchJar, Set<WorkflowRunnerNotification> configuredNotifications, AlertsHandler providedHandler, String remote, String implementationBuild) throws IOException {
    return delegate.initializeInternal(name, scopeId, description, appType, host, username, pool, priority, launchDir, launchJar, configuredNotifications, providedHandler, remote, implementationBuild);
  }

  @Override
  public MonitoredPersistence prepare(INITIALIZED initialized, DirectedGraph<IStep, DefaultEdge> flatSteps) {
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
