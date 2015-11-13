package com.liveramp.workflow_state;

import java.util.List;

//  TODO remove this class, replace usages with MapreduceJob
public class MapReduceJob {

  private final String jobId;
  private final String jobName;
  private final String trackingURL;
  private final List<Counter> counters;

  public static class Counter {
    private final String group;
    private final String name;
    private final Long value;

    public Counter(String group, String name, Long value) {
      this.group = group;
      this.name = name;
      this.value = value;
    }

    public String getGroup() {
      return group;
    }

    public String getName() {
      return name;
    }

    public Long getValue() {
      return value;
    }
  }

  public MapReduceJob(String jobId, String jobName, String trackingURL,
                      List<Counter> counters) {
    this.jobId = jobId;
    this.jobName = jobName;
    this.trackingURL = trackingURL;
    this.counters = counters;
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

  public List<Counter> getCounters() {
    return counters;
  }

}
