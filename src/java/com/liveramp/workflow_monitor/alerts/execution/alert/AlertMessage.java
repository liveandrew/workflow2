package com.liveramp.workflow_monitor.alerts.execution.alert;

import com.liveramp.workflow_state.WorkflowRunnerNotification;

public class AlertMessage {

  private final String mesasage;
  private final WorkflowRunnerNotification notification;

  public AlertMessage(String mesasage, WorkflowRunnerNotification notification) {
    this.mesasage = mesasage;
    this.notification = notification;
  }

  public String getMesasage() {
    return mesasage;
  }

  public WorkflowRunnerNotification getNotification() {
    return notification;
  }

  @Override
  public String toString() {
    return "AlertMessage{" +
        "mesasage='" + mesasage + '\'' +
        ", notification=" + notification +
        '}';
  }
}
