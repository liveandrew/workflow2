package com.rapleaf.cascading_ext.workflow2.state;

public class StepState {

  //  immutable
  private final String actionClass;

  //  required
  private StepStatus status;

  private String statusMessage;
  private String failureMessage;
  private String failureTrace;

  public StepState(StepStatus status, String actionClass) {
    this.status = status;
    this.actionClass = actionClass;
    this.statusMessage = "";
  }

  public StepStatus getStatus() {
    return status;
  }

  public void setStatus(StepStatus status) {
    this.status = status;
  }

  public String getStatusMessage() {
    return statusMessage;
  }

  public void setStatusMessage(String statusMessage) {
    this.statusMessage = statusMessage;
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
