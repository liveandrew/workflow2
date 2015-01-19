package com.rapleaf.cascading_ext.workflow2.state;

import java.util.Map;

public class WorkflowState {

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
