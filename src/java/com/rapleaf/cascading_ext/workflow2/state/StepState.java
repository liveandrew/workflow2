package com.rapleaf.cascading_ext.workflow2.state;

import java.util.Map;

import com.google.common.collect.Maps;

public class StepState {

  //  immutable
  private final String actionClass;

  //  required
  private StepStatus status;

  private String statusMessage;
  private String failureMessage;
  private String failureTrace;

  private Map<String, MapReduceJob> mrJobsByID = Maps.newHashMap();

  public StepState(StepStatus status, String actionClass) {
    this.status = status;
    this.actionClass = actionClass;
    this.statusMessage = "";
  }

  public StepStatus getStatus() {
    return status;
  }

  protected void setStatus(StepStatus status) {
    this.status = status;
  }
  protected void setStatusMessage(String statusMessage) {
    this.statusMessage = statusMessage;
  }
  protected void setFailureMessage(String failureMessage) {
    this.failureMessage = failureMessage;
  }
  protected void setFailureTrace(String failureTrace) {
    this.failureTrace = failureTrace;
  }
  public void addMrjob(MapReduceJob job) {
    this.mrJobsByID.put(job.getJobId(), job);
  }

  public String getFailureMessage() {
    return failureMessage;
  }
  public String getFailureTrace() {
    return failureTrace;
  }
  public String getStatusMessage() {
    return statusMessage;
  }
  public String getActionClass() {
    return actionClass;
  }
  public Map<String, MapReduceJob> getMrJobsByID() {
    return mrJobsByID;
  }


}
