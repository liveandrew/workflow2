package com.liveramp.workflow.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.state.TaskSummary;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_state.StepState;
import com.liveramp.workflow_state.StepStatus;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.types.person_data.WorkflowAttemptStatus;

public class ForwardingPersistence implements WorkflowStatePersistence {

  protected final WorkflowStatePersistence delegatePersistence;

  public ForwardingPersistence(WorkflowStatePersistence delegatePersistence) {
    this.delegatePersistence = delegatePersistence;
  }

  @Override
  public void markWorkflowStarted() throws IOException {
    delegatePersistence.markWorkflowStarted();
  }

  @Override
  public void markWorkflowStopped() throws IOException {
    delegatePersistence.markWorkflowStopped();
  }

  @Override
  public void markStepReverted(String stepToken) throws IOException {
    delegatePersistence.markStepReverted(stepToken);
  }

  @Override
  public void markPool(String pool) throws IOException {
    delegatePersistence.markPool(pool);
  }

  @Override
  public void markPriority(String priority) throws IOException {
    delegatePersistence.markPriority(priority);
  }

  @Override
  public void markShutdownRequested(String reason) throws IOException {
    delegatePersistence.markShutdownRequested(reason);
  }

  @Override
  public StepStatus getStatus(String stepToken) throws IOException {
    return delegatePersistence.getStatus(stepToken);
  }

  @Override
  public Map<String, StepStatus> getStepStatuses() throws IOException {
    return delegatePersistence.getStepStatuses();
  }

  @Override
  public Map<String, StepState> getStepStates() throws IOException {
    return delegatePersistence.getStepStates();
  }

  @Override
  public String getShutdownRequest() throws IOException {
    return delegatePersistence.getShutdownRequest();
  }

  @Override
  public String getPriority() throws IOException {
    return delegatePersistence.getPriority();
  }

  @Override
  public String getPool() throws IOException {
    return delegatePersistence.getPool();
  }

  @Override
  public String getName() throws IOException {
    return delegatePersistence.getName();
  }

  @Override
  public String getScopeIdentifier() throws IOException {
    return delegatePersistence.getScopeIdentifier();
  }

  @Override
  public String getId() throws IOException {
    return delegatePersistence.getId();
  }

  @Override
  public WorkflowAttemptStatus getStatus() throws IOException {
    return delegatePersistence.getStatus();
  }

  @Override
  public List<AlertsHandler> getRecipients(WorkflowRunnerNotification notification) throws IOException {
    return delegatePersistence.getRecipients(notification);
  }

  @Override
  public long getExecutionId() throws IOException {
    return delegatePersistence.getExecutionId();
  }

  @Override
  public long getAttemptId() throws IOException {
    return delegatePersistence.getAttemptId();
  }

  @Override
  public ThreeNestedMap<String, String, String, Long> getCountersByStep() throws IOException {
    return delegatePersistence.getCountersByStep();
  }

  @Override
  public TwoNestedMap<String, String, Long> getFlatCounters() throws IOException {
    return delegatePersistence.getFlatCounters();
  }


  @Override
  public void markStepStatusMessage(String stepToken, String newMessage) throws IOException {
    delegatePersistence.markStepStatusMessage(stepToken, newMessage);
  }

  @Override
  public void markStepRunningJob(String stepToken, String jobId, String jobName, String trackingURL) throws IOException {
    delegatePersistence.markStepRunningJob(stepToken, jobId, jobName, trackingURL);
  }

  @Override
  public void markJobCounters(String stepToken, String jobId, TwoNestedMap<String, String, Long> values) throws IOException {
    delegatePersistence.markJobCounters(stepToken, jobId, values);
  }

  @Override
  public void markJobTaskInfo(String stepToken, String jobId, TaskSummary info) throws IOException {
    delegatePersistence.markJobTaskInfo(stepToken, jobId, info);
  }

  @Override
  public void markStepRunning(String stepToken) throws IOException {
    delegatePersistence.markStepRunning(stepToken);
  }

  @Override
  public void markStepFailed(String stepToken, Throwable t) throws IOException {
    delegatePersistence.markStepFailed(stepToken, t);
  }

  @Override
  public void markStepCompleted(String stepToken) throws IOException {
    delegatePersistence.markStepCompleted(stepToken);
  }

}
