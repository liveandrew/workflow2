package com.liveramp.workflow_monitor.alerts.execution.alert;

import java.io.IOException;

import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.databases.workflow_db.models.WorkflowAlert;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

public class AlertMessage {

  private final String classname;
  private final String message;
  private final WorkflowRunnerNotification notification;

  public AlertMessage(String classname, String message, WorkflowRunnerNotification notification) {
    this.classname = classname;
    this.message = message;
    this.notification = notification;
  }

  public String getMessage() {
    return message;
  }

  public WorkflowRunnerNotification getNotification() {
    return notification;
  }

  public String getClassname() {
    return classname;
  }

  public static AlertMessage createAlertMessage(String classname, String message, WorkflowRunnerNotification notification,
                                                WorkflowExecution execution, IDatabases db) throws IOException {
    IWorkflowDb wfDb = db.getWorkflowDb();

    WorkflowAlert wa = wfDb.workflowAlerts().create(classname, message);
    wfDb.workflowAlertWorkflowExecutions().create(wa.getId(), execution.getId());
    return new AlertMessage(classname, message, notification);
  }

  public static AlertMessage createAlertMessage(String classname, String message, WorkflowRunnerNotification notification,
                                                MapreduceJob job, IDatabases db) throws IOException {

    IWorkflowDb wfDb = db.getWorkflowDb();
    WorkflowAlert wa = wfDb.workflowAlerts().create(classname, message);
    wfDb.workflowAlertMapreduceJobs().create(wa.getId(), job.getId());
    return new AlertMessage(classname, message, notification);
  }

  @Override
  public String toString() {
    return "AlertMessage{" +
        "classname=" + classname + "\'" +
        "message='" + message + '\'' +
        ", notification=" + notification +
        '}';
  }
}
