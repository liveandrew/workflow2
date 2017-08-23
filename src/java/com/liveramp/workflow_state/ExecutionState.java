package com.liveramp.workflow_state;

public class ExecutionState {

  private final Long startTime;
  private final Long endTime;
  private final String appName;

  public ExecutionState(Long startTime, Long endTime, String appName) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.appName = appName;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public String getAppName() {
    return appName;
  }
}
