package com.liveramp.workflow_monitor.alerts.execution.alert;

public class ExecutionAlert {

  private final long execution;
  private final AlertMessage message;

  public ExecutionAlert(long execution, AlertMessage message) {
    this.execution = execution;
    this.message = message;
  }

  public long getExecution() {
    return execution;
  }

  public AlertMessage getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "ExecutionAlert{" +
        "execution=" + execution +
        ", message=" + message +
        '}';
  }
}
