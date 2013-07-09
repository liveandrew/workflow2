package com.rapleaf.cascading_ext.workflow2;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public class WorkflowRunnerNotificationSet {

  private Set<WorkflowRunnerNotification> notifications;

  public WorkflowRunnerNotificationSet() {
    this.notifications = EnumSet.allOf(WorkflowRunnerNotification.class);
  }

  public WorkflowRunnerNotificationSet set(WorkflowRunnerNotification... enabledNotifications) {
    this.notifications = new HashSet<WorkflowRunnerNotification>(Arrays.asList(enabledNotifications));
    return this;
  }

  public WorkflowRunnerNotificationSet set(Set<WorkflowRunnerNotification> enabledNotifications) {
    this.notifications = enabledNotifications;
    return this;
  }

  public WorkflowRunnerNotificationSet enable(WorkflowRunnerNotification notification) {
    this.notifications.add(notification);
    return this;
  }

  public WorkflowRunnerNotificationSet only(WorkflowRunnerNotification notification) {
    this.notifications = new HashSet<WorkflowRunnerNotification>();
    this.notifications.add(notification);
    return this;
  }

  public WorkflowRunnerNotificationSet disable(WorkflowRunnerNotification notification) {
    this.notifications.remove(notification);
    return this;
  }

  public Set<WorkflowRunnerNotification> get() {
    return notifications;
  }
}
