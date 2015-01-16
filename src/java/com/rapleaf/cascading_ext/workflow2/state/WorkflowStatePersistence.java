package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.rapleaf.cascading_ext.workflow2.Step;

public interface WorkflowStatePersistence {

  public static final Set<StepStatus> NON_BLOCKING = EnumSet.of(
      StepStatus.COMPLETED, StepStatus.SKIPPED
  );


  public StepState getState(String stepToken);
  public WorkflowState getFlowStatus();

  public void markStepRunning(String stepToken) throws IOException;
  public void markStepFailed(String stepToken, Throwable t) throws IOException;
  public void markStepSkipped(String stepToken) throws IOException;
  public void markStepCompleted(String stepToken) throws IOException;

  public void prepare(DirectedGraph<Step, DefaultEdge> flatSteps);

  public void markShutdownRequested(String reason);
  public void markWorkflowStopped();

  public static class WorkflowState {

    private final Map<String, StepState> stepStatuses;
    private final String shutdownRequest;

    public WorkflowState(Map<String, StepState> stepStatuses, String shutdownRequest) {
      this.stepStatuses = stepStatuses;
      this.shutdownRequest = shutdownRequest;
    }

    public Map<String, StepState> getStepStatuses() {
      return stepStatuses;
    }

    public String getShutdownRequest() {
      return shutdownRequest;
    }

  }

}
