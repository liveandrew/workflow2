package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import com.liveramp.workflow_service.generated.StepExecuteStatus;
import com.liveramp.workflow_service.generated.WorkflowDefinition;

public interface WorkflowStatePersistence {

  public static final Set<StepExecuteStatus._Fields> NON_BLOCKING = EnumSet.of(
      StepExecuteStatus._Fields.COMPLETED, StepExecuteStatus._Fields.SKIPPED
  );


  public StepExecuteStatus getStatus(String stepToken);
  public WorkflowState getFlowStatus();

  public void markStepRunning(String stepToken) throws IOException;
  public void markStepFailed(String stepToken, Throwable t) throws IOException;
  public void markStepSkipped(String stepToken) throws IOException;
  public void markStepCompleted(String stepToken) throws IOException;

  public void prepare(WorkflowDefinition plannedFlow);

  public void markShutdownRequested(String reason);
  public void markWorkflowStopped();

  public static class WorkflowState {

    private final Map<String, StepExecuteStatus> stepStatuses;
    private final String shutdownRequest;

    public WorkflowState(Map<String, StepExecuteStatus> stepStatuses, String shutdownRequest) {
      this.stepStatuses = stepStatuses;
      this.shutdownRequest = shutdownRequest;
    }

    public Map<String, StepExecuteStatus> getStepStatuses() {
      return stepStatuses;
    }

    public String getShutdownRequest() {
      return shutdownRequest;
    }

  }

}
