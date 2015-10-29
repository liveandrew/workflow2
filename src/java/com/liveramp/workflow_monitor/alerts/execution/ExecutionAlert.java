package com.liveramp.workflow_monitor.alerts.execution;

import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

public class ExecutionAlert {

  private final long execution;
  private final String mesasage;
  private final WorkflowRunnerNotification notification;

  public ExecutionAlert(long execution, String mesasage, WorkflowRunnerNotification notification) {
    this.execution = execution;
    this.mesasage = mesasage;
    this.notification = notification;
  }

  public long getExecution() {
    return execution;
  }

  public String getMesasage() {
    return mesasage;
  }

  public WorkflowRunnerNotification getNotification() {
    return notification;
  }


  @Override
  public String toString() {
    return "ExecutionAlert{" +
        "execution=" + execution +
        ", mesasage='" + mesasage + '\'' +
        ", notification=" + notification +
        '}';
  }
}
