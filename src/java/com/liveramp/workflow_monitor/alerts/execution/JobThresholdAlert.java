package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

public abstract class JobThresholdAlert extends MapreduceJobAlertGenerator {

  private final WorkflowRunnerNotification notification;
  private final double threshold;

  protected JobThresholdAlert(double threshold,
                              WorkflowRunnerNotification notification,
                              Multimap<String, String> countersToFetch) {
    super(countersToFetch);
    this.notification = notification;
    this.threshold = threshold;
  }

  @Override
  public List<AlertMessage> generateAlert(MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException {


    Double value = calculateStatistic(counters);

    if (value != null && value > threshold) {

      String message = getMessage(value) + "\n\n" +
          "Job name: " +
          job.getJobName() +
          "Tracker link: " +
          job.getTrackingUrl() + "\n";

      return Lists.newArrayList(
          new AlertMessage(message, notification)
      );

    }

    return Lists.newArrayList();
  }

  protected abstract Double calculateStatistic(TwoNestedMap<String, String, Long> counters);

  protected abstract String getMessage(double value);

}
