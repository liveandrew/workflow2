package com.liveramp.workflow_monitor.alerts.execution;

import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;

public class ExecutionAlert {

  private final long execution;
  private final String mesasage;
  private final AlertSeverity severity;

  public ExecutionAlert(long execution, String mesasage, AlertSeverity severity) {
    this.execution = execution;
    this.mesasage = mesasage;
    this.severity = severity;
  }

  public long getExecution() {
    return execution;
  }

  public String getMesasage() {
    return mesasage;
  }

  public AlertSeverity getSeverity() {
    return severity;
  }

  @Override
  public String toString() {
    return "ExecutionAlert{" +
        "execution=" + execution +
        ", mesasage='" + mesasage + '\'' +
        ", severity=" + severity +
        '}';
  }
}
