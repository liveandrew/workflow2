package com.rapleaf.cascading_ext.workflow2.state;

public class MapReduceJob {

  private final String jobId;
  private final String jobName;
  private final String trackingURL;

  public MapReduceJob(String jobId, String jobName, String trackingURL) {
    this.jobId = jobId;
    this.jobName = jobName;
    this.trackingURL = trackingURL;
  }

  public String getJobId() {
    return jobId;
  }

  public String getJobName() {
    return jobName;
  }

  public String getTrackingURL() {
    return trackingURL;
  }
}
