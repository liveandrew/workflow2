package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import com.liveramp.cascading_ext.flow.JobPersister;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.state.LaunchedJob;
import com.liveramp.java_support.workflow.TaskSummary;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;

public class WorkflowJobPersister implements JobPersister {

  private final WorkflowStatePersistence persistence;
  private final String checkpoint;
  private final CounterFilter filter;

  public WorkflowJobPersister(WorkflowStatePersistence persistence, String checkpoint, CounterFilter filter) {
    this.persistence = persistence;
    this.checkpoint = checkpoint;
    this.filter = filter;
  }

  @Override
  public void onRunning(LaunchedJob launchedJob) throws IOException {
    persistence.markStepRunningJob(
        checkpoint,
        launchedJob.getJobId(),
        launchedJob.getJobName(),
        launchedJob.getTrackingURL()
    );
  }

  @Override
  public void onTaskInfo(String jobID, TaskSummary summary) throws IOException {
    persistence.markJobTaskInfo(checkpoint, jobID, summary);
  }

  @Override
  public void onCounters(String jobID, TwoNestedMap<String, String, Long> counters) throws IOException {

    TwoNestedMap<String, String, Long> toRecord = new TwoNestedMap<>();
    for (String group : counters.key1Set()) {
      for (String name : counters.key2Set(group)) {
        if (filter.isRecord(group, name)) {
          toRecord.put(group, name, counters.get(group, name));
        }
      }
    }

    persistence.markJobCounters(checkpoint, jobID, counters);
  }

}
