package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;
import java.text.DecimalFormat;

import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.databases.workflow_db.models.StepAttempt;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

public abstract class JobThresholdAlert extends MapreduceJobAlertGenerator {

  private final WorkflowRunnerNotification notification;
  private final double threshold;

  private static final DecimalFormat df = new DecimalFormat("#.##");

  protected JobThresholdAlert(double threshold,
                              WorkflowRunnerNotification notification,
                              Multimap<String, String> countersToFetch) {
    super(countersToFetch);
    this.notification = notification;
    this.threshold = threshold;
  }

  protected String asPercent(double fraction) {
    return df.format(fraction * 100) + "%";
  }

  @Override
  public AlertMessage generateAlert(StepAttempt stepAttempt, MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException {

    Double value = calculateStatistic(counters);

    if (value != null && value > threshold) {

      String message = "Step: " +
          stepAttempt.getStepToken() + "\n" +
          "Job name: " +
          job.getJobName() + "\n" +
          "Tracker link: " +
          job.getTrackingUrl() + "\n\n" +
          getMessage(value);

      return new AlertMessage(message, notification);
    }

    return null;
  }

  protected abstract Double calculateStatistic(TwoNestedMap<String, String, Long> counters);

  protected abstract String getMessage(double value);

}
