package com.rapleaf.cascading_ext.workflow2.state;

public class StepState {

  //  immutable
  private final String actionClass;

  //  required
  private StepStatus status;

  private String failureMessage;
  private String failureTrace;

  public StepState(StepStatus status, String actionClass) {
    this.status = status;
    this.actionClass = actionClass;
  }

  public StepStatus getStatus() {
    return status;
  }

  public void setStatus(StepStatus status) {
    this.status = status;
  }

  public String getFailureMessage() {
    return failureMessage;
  }

  public void setFailureMessage(String failureMessage) {
    this.failureMessage = failureMessage;
  }

  public String getFailureTrace() {
    return failureTrace;
  }

  public void setFailureTrace(String failureTrace) {
    this.failureTrace = failureTrace;
  }

  public String getActionClass() {
    return actionClass;
  }

}
