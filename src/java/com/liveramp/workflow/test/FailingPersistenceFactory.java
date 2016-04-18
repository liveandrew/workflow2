package com.liveramp.workflow.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.state.TaskSummary;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.functional.Fn;
import com.liveramp.java_support.functional.Fns;
import com.liveramp.workflow_state.AttemptStatus;
import com.liveramp.workflow_state.StepState;
import com.liveramp.workflow_state.StepStatus;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public class FailingPersistenceFactory implements WorkflowPersistenceFactory {

  private final WorkflowPersistenceFactory delegate;
  private final Set<String> stepsToFailFullNames;


  public FailingPersistenceFactory(WorkflowPersistenceFactory delegate, StepNameBuilder stepNameBuilder) {
    this(delegate, Sets.newHashSet(stepNameBuilder));
  }

  /**
   * @param stepNameBuilders any given steps are failed after they finish executing
   */
  public FailingPersistenceFactory(WorkflowPersistenceFactory delegate, Set<StepNameBuilder> stepNameBuilders) {
    this.delegate = delegate;
    this.stepsToFailFullNames = Sets.newHashSet(Fns.map(new Fn<StepNameBuilder, String>() {
      @Override
      public String apply(StepNameBuilder input) {
        return input.getCompositeStepName();
      }
    }, stepNameBuilders));
  }

  @Override
  public WorkflowStatePersistence prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                                          String name,
                                          String scopeId,
                                          String description,
                                          AppType appType,
                                          String host,
                                          String username,
                                          String pool,
                                          String priority,
                                          String launchDir,
                                          String launchJar,
                                          Set<WorkflowRunnerNotification> configuredNotifications,
                                          AlertsHandler configuredHandler,
                                          String remote,
                                          String implementationBuild) {
    return new FailingPersistence(delegate.prepare(flatSteps, name, scopeId, description, appType, host, username, pool, priority, launchDir, launchJar, configuredNotifications, configuredHandler, remote, implementationBuild), stepsToFailFullNames);
  }

  public static class IntentionallyFailedStepException extends RuntimeException {
    public IntentionallyFailedStepException(String message) {
      super(message);
    }
  }

  private static class FailingPersistence implements WorkflowStatePersistence {

    private final WorkflowStatePersistence delegate;
    private final Set<String> stepsToFailFullNames;

    private FailingPersistence(WorkflowStatePersistence delegate, Set<String> stepsToFailFullNames) {
      this.delegate = delegate;
      this.stepsToFailFullNames = stepsToFailFullNames;
    }

    @Override
    public void markStepStatusMessage(String stepToken, String newMessage) throws IOException {
      delegate.markStepStatusMessage(stepToken, newMessage);
    }

    @Override
    public void markStepRunningJob(String stepToken, String jobId, String jobName, String trackingURL) throws IOException {
      delegate.markStepRunningJob(stepToken, jobId, jobName, trackingURL);
    }

    @Override
    public void markJobCounters(String stepToken, String jobId, TwoNestedMap<String, String, Long> values) throws IOException {
      delegate.markJobCounters(stepToken, jobId, values);
    }

    @Override
    public void markJobTaskInfo(String stepToken, String jobId, TaskSummary info) throws IOException {
      delegate.markJobTaskInfo(stepToken, jobId, info);
    }

    @Override
    public void markStepRunning(String stepToken) throws IOException {
      delegate.markStepRunning(stepToken);
    }

    @Override
    public void markStepFailed(String stepToken, Throwable t) throws IOException {
      delegate.markStepFailed(stepToken, t);
    }

    @Override
    public void markStepCompleted(String stepToken) throws IOException {
      delegate.markStepCompleted(stepToken);
      if (stepsToFailFullNames.contains(stepToken)) {
        IntentionallyFailedStepException exception = new IntentionallyFailedStepException(String.format("Failed step intentionally: %s", stepToken));
        delegate.markStepFailed(stepToken, exception);
        throw exception;
      }
    }

    @Override
    public void markWorkflowStarted() throws IOException {
      delegate.markWorkflowStarted();
    }

    @Override
    public void markWorkflowStopped() throws IOException {
      delegate.markWorkflowStopped();
    }

    @Override
    public void markStepReverted(String stepToken) throws IOException {
      delegate.markStepReverted(stepToken);
    }

    @Override
    public void markPool(String pool) throws IOException {
      delegate.markPool(pool);
    }

    @Override
    public void markPriority(String priority) throws IOException {
      delegate.markPriority(priority);
    }

    @Override
    public void markShutdownRequested(String reason) throws IOException {
      delegate.markShutdownRequested(reason);
    }

    @Override
    public StepStatus getStatus(String stepToken) throws IOException {
      return delegate.getStatus(stepToken);
    }

    @Override
    public Map<String, StepStatus> getStepStatuses() throws IOException {
      return delegate.getStepStatuses();
    }

    @Override
    public Map<String, StepState> getStepStates() throws IOException {
      return delegate.getStepStates();
    }

    @Override
    public String getShutdownRequest() throws IOException {
      return delegate.getShutdownRequest();
    }

    @Override
    public String getPriority() throws IOException {
      return delegate.getPriority();
    }

    @Override
    public String getPool() throws IOException {
      return delegate.getPool();
    }

    @Override
    public String getName() throws IOException {
      return delegate.getName();
    }

    @Override
    public String getScopeIdentifier() throws IOException {
      return delegate.getScopeIdentifier();
    }

    @Override
    public String getId() throws IOException {
      return delegate.getId();
    }

    @Override
    public AttemptStatus getStatus() throws IOException {
      return delegate.getStatus();
    }

    @Override
    public List<AlertsHandler> getRecipients(WorkflowRunnerNotification notification) throws IOException {
      return delegate.getRecipients(notification);
    }

    @Override
    public long getExecutionId() throws IOException {
      return delegate.getExecutionId();
    }

    @Override
    public long getAttemptId() throws IOException {
      return delegate.getAttemptId();
    }

    @Override
    public ThreeNestedMap<String, String, String, Long> getCountersByStep() throws IOException {
      return delegate.getCountersByStep();
    }

    @Override
    public TwoNestedMap<String, String, Long> getFlatCounters() throws IOException {
      return delegate.getFlatCounters();
    }
  }
}
