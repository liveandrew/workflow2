package com.rapleaf.cascading_ext.workflow2;

import java.util.EnumSet;
import java.util.Set;

public class WorkflowRunnerNotificationSet {

  private Set<WorkflowRunnerNotification> notifications;

  public static WorkflowRunnerNotificationSet all() {
    return new WorkflowRunnerNotificationSet(EnumSet.allOf(WorkflowRunnerNotification.class));
  }

  public static WorkflowRunnerNotificationSet none() {
    return new WorkflowRunnerNotificationSet(EnumSet.noneOf(WorkflowRunnerNotification.class));
  }

  public static WorkflowRunnerNotificationSet only(WorkflowRunnerNotification notification,
                                                   WorkflowRunnerNotification... notifications) {
    return new WorkflowRunnerNotificationSet(EnumSet.of(notification, notifications));
  }

  public static WorkflowRunnerNotificationSet except(WorkflowRunnerNotification notification,
                                                     WorkflowRunnerNotification... notifications) {
    return new WorkflowRunnerNotificationSet(EnumSet.complementOf(EnumSet.of(notification, notifications)));
  }

  public WorkflowRunnerNotificationSet(EnumSet<WorkflowRunnerNotification> notifications) {
    this.notifications = notifications;
  }

  public Set<WorkflowRunnerNotification> get() {
    return notifications;
  }
}
