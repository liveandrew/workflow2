package com.rapleaf.cascading_ext.workflow2;

public class WorkflowRunnerOptions {

  private int maxConcurrentSteps;
  private Integer webUiPort;
  private String notificationEmails;

  public WorkflowRunnerOptions() {
    maxConcurrentSteps = Integer.MAX_VALUE;
    webUiPort = null;
    notificationEmails = null;
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

  public String getNotificationEmails() {
    return notificationEmails;
  }

  public WorkflowRunnerOptions setNotificationEmails(String notificationEmails) {
    this.notificationEmails = notificationEmails;
    return this;
  }
}
