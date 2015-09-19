package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.hp.gagawa.java.elements.A;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.java_support.alerts_handler.recipients.TeamEmailList;
import com.liveramp.workflow_monitor.alerts.execution.recipient.RecipientGenerator;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowConstants;

public class ExecutionAlerter {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionAlerter.class);

  private final Multimap<Long, Class<? extends ExecutionAlertGenerator>> sentProdAlerts = HashMultimap.create();

  private final List<ExecutionAlertGenerator> alerts;
  private final RecipientGenerator generator;
  private final IDatabases db;

  public ExecutionAlerter(RecipientGenerator generator, List<ExecutionAlertGenerator> alerts, IDatabases db) {
    this.alerts = alerts;
    this.generator = generator;
    this.db = db;
  }

  public void generateAlerts() throws IOException, URISyntaxException {
    for (ExecutionAlertGenerator alertGenerator : alerts) {
      Class<? extends ExecutionAlertGenerator> alertClass = alertGenerator.getClass();
      LOG.info("Running generator " + alertClass);

      for (ExecutionAlert genAlert : alertGenerator.generateAlerts(db)) {
        long executionId = genAlert.getExecution();

        if (!sentProdAlerts.containsEntry(executionId, alertClass)) {
          LOG.info("Sending alert: " + genAlert);

          WorkflowExecution execution = db.getRlDb().workflowExecutions().find(executionId);
          String recipientEmail = generator.getRecipient(genAlert.getSeverity(), execution);

          LOG.info("Notifying: " + recipientEmail);

          if (recipientEmail != null) {

            ArrayList<String> tags = Lists.newArrayList(WorkflowConstants.WORKFLOW_EMAIL_SUBJECT_TAG);
            if (genAlert.getSeverity() == AlertSeverity.ERROR) {
              tags.add(WorkflowConstants.ERROR_EMAIL_SUBJECT_TAG);
            }

            AlertsHandler handler = AlertsHandlers.builder(TeamEmailList.NULL)
                .setEngineeringRecipient(AlertRecipients.of(Lists.newArrayList(recipientEmail)))
                .setTags(tags)
                .build();

            handler.sendAlert(
                buildSubject(alertClass.getSimpleName(), execution),
                buildMessage(genAlert.getMesasage(), execution),
                AlertRecipients.engineering(genAlert.getSeverity())
            );

            sentProdAlerts.put(executionId, alertClass);

          } else {
            LOG.info("No notification email found for execution " + execution.getId() + " (" + execution.getApplication().getName() + ")");
          }
        } else {
          LOG.info("Not re-notifying about execution " + executionId + " alert gen " + alertClass);
        }

      }
    }
  }

  private String buildSubject(String alertMessage, WorkflowExecution execution) {
    String[] split = execution.getName().split("\\.");

    String message = alertMessage + ": " + split[split.length - 1];

    if (execution.getScopeIdentifier() != null) {
      message = message + " (" + execution.getScopeIdentifier() + ")";
    }

    return message;
  }

  private String buildMessage(String alertMessage, WorkflowExecution execution) throws URISyntaxException, UnsupportedEncodingException {

    A executionLink = new A()
        .setHref(new URIBuilder()
            .setScheme("http")
            .setHost("workflows.liveramp.net")
            .setPath("/execution.html")
            .setParameter("id", Long.toString(execution.getId()))
            .build().toString())
        .appendText(Long.toString(execution.getId()));

    A appLink = new A()
        .setHref(new URIBuilder()
            .setScheme("http")
            .setHost("workflows.liveramp.net")
            .setPath("/application.html")
            .setParameter("name", URLEncoder.encode(execution.getName(), "UTF-8"))
            .build().toString())
        .appendText(execution.getName());

    return "Application: " + appLink.write() +
        "\nExecution: " + executionLink.write() +
        "\n\n" + alertMessage;

  }

}
