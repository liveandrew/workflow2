package com.rapleaf.cascading_ext.workflow2;

import java.util.*;

public class WorkflowRunnerOptions {

  private int maxConcurrentSteps;
  private Integer webUiPort;
  private List<String> notificationRecipients;
  private WorkflowRunnerNotificationSet enabledNotifications;

  public WorkflowRunnerOptions() {
    maxConcurrentSteps = Integer.MAX_VALUE;
    webUiPort = null;
    notificationRecipients = null;
    enabledNotifications = new WorkflowRunnerNotificationSet();
  }

  public int getMaxConcurrentSteps() {
    return maxConcurrentSteps;
  }

  public WorkflowRunnerOptions setMaxConcurrentSteps(int maxConcurrentSteps) {
    this.maxConcurrentSteps = maxConcurrentSteps;
    return this;
  }

  public Integer getWebUiPort() {
    return webUiPort;
  }

  public WorkflowRunnerOptions setWebUiPort(Integer webUiPort) {
    this.webUiPort = webUiPort;
    return this;
  }

  public List<String> getNotificationRecipients() {
    return notificationRecipients;
  }

  public WorkflowRunnerOptions setNotificationRecipients(String... notificationRecipients) {
    this.notificationRecipients = Arrays.asList(notificationRecipients);
    return this;
  }

  public WorkflowRunnerOptions setNotificationRecipients(List<String> notificationEmails) {
    this.notificationRecipients = notificationEmails;
    return this;
  }

  public WorkflowRunnerOptions setEnabledNotifications(WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications.set(enabledNotifications);
    return this;
  }

  public WorkflowRunnerOptions setEnabledNotifications(Set<WorkflowRunnerNotification> enabledNotifications) {
    this.enabledNotifications.set(enabledNotifications);
    return this;
  }

  public WorkflowRunnerOptions enableNotification(WorkflowRunnerNotification notification) {
    this.enabledNotifications.enable(notification);
    return this;
  }

  public WorkflowRunnerOptions enableNotificationOnly(WorkflowRunnerNotification notification) {
    this.enabledNotifications.only(notification);
    return this;
  }

  public WorkflowRunnerOptions disableNotification(WorkflowRunnerNotification notification) {
    this.enabledNotifications.disable(notification);
    return this;
  }

  public WorkflowRunnerNotificationSet getEnabledNotifications() {
    return enabledNotifications;
  }
}
