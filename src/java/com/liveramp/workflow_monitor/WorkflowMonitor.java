package com.liveramp.workflow_monitor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlerter;

public class WorkflowMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowMonitor.class);

  public static final int FIVE_MINUTES = 5 * 60 * 1000;

  private final List<ExecutionAlerter> executionAlerters;

  public WorkflowMonitor(List<ExecutionAlerter> executionAlerters) {
    this.executionAlerters = executionAlerters;
  }

  public void monitor(){

    try {

      while (true) {
        LOG.info("Starting to generate alerts");

        for (ExecutionAlerter alerter : executionAlerters) {
          alerter.generateAlerts();
        }

        LOG.info("Sleeping for " + FIVE_MINUTES + "ms");
        Thread.sleep(FIVE_MINUTES);
      }

    } catch (Exception e) {
      LOG.info("Failure", e);
      AlertsHandlers.devTools(WorkflowMonitor.class).sendAlert("WorkflowMonitor failed!", e,
          AlertRecipients.engineering(AlertSeverity.ERROR)
      );
    }

  }


}

