package com.liveramp.workflow.test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_state.StepStatus;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public class MonitoredPersistenceFactory implements WorkflowPersistenceFactory {

  private final WorkflowPersistenceFactory delegate;

  public MonitoredPersistenceFactory(WorkflowPersistenceFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public MonitoredPersistence prepare(DirectedGraph<Step, DefaultEdge> flatSteps, String name, String scopeId, String description, AppType appType, String host, String username, String pool, String priority, String launchDir, String launchJar, Set<WorkflowRunnerNotification> configuredNotifications, AlertsHandler configuredHandler, String remote, String implementationBuild) {
    return new MonitoredPersistence(delegate.prepare(flatSteps, name, scopeId, description, appType, host, username, pool, priority, launchDir, launchJar, configuredNotifications, configuredHandler, remote, implementationBuild));
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
