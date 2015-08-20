package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.TeamEmailList;
import com.liveramp.workflow_monitor.alerts.execution.recipient.RecipientGenerator;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

public class ExecutionAlerter {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionAlerter.class);

  private final Multimap<Long, Class<? extends ExecutionAlertGenerator>> sentProdAlerts = HashMultimap.create();

  private final List<ExecutionAlertGenerator> alerts;
  private final RecipientGenerator generator;
  private final IDatabases db;

  public ExecutionAlerter(RecipientGenerator generator, List<ExecutionAlertGenerator> alerts, IDatabases db){
    this.alerts = alerts;
    this.generator = generator;
    this.db = db;
  }

  public void generateAlerts() throws IOException {
    for (ExecutionAlertGenerator alertGenerator : alerts) {
      Class<? extends ExecutionAlertGenerator> alertClass = alertGenerator.getClass();
      LOG.info("Running generator " + alertClass);

      for (ExecutionAlert genAlert : alertGenerator.generateAlerts(db)) {
        long executionId = genAlert.getExecution();

        if(!sentProdAlerts.containsEntry(executionId, alertClass)) {
          LOG.info("Sending alert: " + genAlert);

          WorkflowExecution execution = db.getRlDb().workflowExecutions().find(executionId);
          String recipientEmail = generator.getRecipient(genAlert.getSeverity(), execution);

          LOG.info("Would email: " + recipientEmail);

          if (recipientEmail != null) {

            AlertsHandler handler = AlertsHandlers.builder(TeamEmailList.DEV_TOOLS)
                .setEngineeringRecipient(AlertRecipients.of(recipientEmail))
                .build();

            handler.sendAlert(alertClass.getSimpleName(),
                genAlert.getMesasage(),
                AlertRecipients.engineering(genAlert.getSeverity())
            );

          } else {
            LOG.info("No notification email found for execution " + execution.getId() + " (" + execution.getApplication().getName() + ")");
          }
        }else{
          LOG.info("Not re-notifying about execution "+executionId+" alert gen "+alertClass);
        }

      }
    }
  }

}
