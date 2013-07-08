package com.rapleaf.cascading_ext.workflow2;

public class WorkflowRunnerOptions {

  private int maxConcurrentComponents;
  private Integer webUiPort;
  private String notificationEmails;

  public WorkflowRunnerOptions() {
    maxConcurrentComponents = Integer.MAX_VALUE;
    webUiPort = null;
    notificationEmails = null;
  }

  public int getMaxConcurrentComponents() {
    return maxConcurrentComponents;
  }

  public WorkflowRunnerOptions setMaxConcurrentComponents(int maxConcurrentComponents) {
    this.maxConcurrentComponents = maxConcurrentComponents;
    return this;
  }

  public Integer getWebUiPort() {
    return webUiPort;
  }

  public WorkflowRunnerOptions setWebUiPort(Integer webUiPort) {
    this.webUiPort = webUiPort;
    return this;
  }

  public String getNotificationEmails() {
    return notificationEmails;
  }

  public WorkflowRunnerOptions setNotificationEmails(String notificationEmails) {
    this.notificationEmails = notificationEmails;
    return this;
  }
}
