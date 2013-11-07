package com.rapleaf.cascading_ext.workflow2;

import java.util.Arrays;
import java.util.List;

public class WorkflowRunnerOptions {

  private int maxConcurrentSteps;
  private Integer webUiPort;
  private List<String> notificationRecipients;
  private WorkflowRunnerNotificationSet enabledNotifications;
  private boolean enableWebUiServer;
  private int statsDPort;
  private String statsDHost;

  public WorkflowRunnerOptions() {
    maxConcurrentSteps = Integer.MAX_VALUE;
    webUiPort = null;
    notificationRecipients = null;
    enabledNotifications = WorkflowRunnerNotificationSet.all();
    enableWebUiServer = true;
    statsDPort = 8125;
    statsDHost = "pglibertyc6";
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

  public WorkflowRunnerOptions setEnabledNotifications(WorkflowRunnerNotification enabledNotification,
      WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = WorkflowRunnerNotificationSet.only(enabledNotification, enabledNotifications);
    return this;
  }

  public WorkflowRunnerOptions setEnabledNotifications(WorkflowRunnerNotificationSet enabledNotifications) {
    this.enabledNotifications = enabledNotifications;
    return this;
  }

  public WorkflowRunnerOptions setEnabledNotificationsExcept(WorkflowRunnerNotification enabledNotification,
      WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = WorkflowRunnerNotificationSet.except(enabledNotification, enabledNotifications);
    return this;
  }

  public WorkflowRunnerNotificationSet getEnabledNotifications() {
    return enabledNotifications;
  }

  public boolean getEnableWebUiServer() {
    return enableWebUiServer;
  }

  public WorkflowRunnerOptions setEnableWebUiServer(boolean enableWebUiServer) {
    this.enableWebUiServer = enableWebUiServer;
    return this;
  }

  public int getStatsDPort() {
    return statsDPort;
  }

  public void setStatsDPort(int statsDPort) {
    this.statsDPort = statsDPort;
  }

  public String getStatsDHost() {
    return statsDHost;
  }

  public void setStatsDHost(String statsDHost) {
    this.statsDHost = statsDHost;
  }
}
