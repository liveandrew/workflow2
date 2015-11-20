package com.liveramp.workflow_state;

import java.util.List;

import com.liveramp.java_support.workflow.TaskSummary;

//  TODO remove this class, replace usages with MapreduceJob
public class MapReduceJob {

  private final String jobId;
  private final String jobName;
  private final String trackingURL;
  private final List<Counter> counters;

  private TaskSummary taskSummary;

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
                      TaskSummary taskSummary,
                      List<Counter> counters) {
    this.jobId = jobId;
    this.jobName = jobName;
    this.trackingURL = trackingURL;
    this.taskSummary = taskSummary;
    this.counters = counters;
  }

  public void setTaskSummary(TaskSummary taskSummary) {
    this.taskSummary = taskSummary;
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

  public TaskSummary getTaskSummary() {
    return taskSummary;
  }
}
