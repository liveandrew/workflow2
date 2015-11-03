package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.hp.gagawa.java.elements.A;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.map.NestedMultimap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.java_support.alerts_handler.AlertMessages;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.liveramp.workflow_monitor.alerts.execution.recipient.RecipientGenerator;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.models.MapreduceCounter;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowConstants;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowQueries;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

public class ExecutionAlerter {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionAlerter.class);

  private final Multimap<Long, Class> sentProdAlerts = HashMultimap.create();

  private final List<ExecutionAlertGenerator> executionAlerts;
  private final List<MapreduceJobAlertGenerator> jobAlerts;

  private final RecipientGenerator generator;
  private final IDatabases db;

  public ExecutionAlerter(RecipientGenerator generator,
                          List<ExecutionAlertGenerator> executionAlerts,
                          List<MapreduceJobAlertGenerator> jobAlerts,
                          IDatabases db) {
    this.executionAlerts = executionAlerts;
    this.jobAlerts = jobAlerts;
    this.generator = generator;
    this.db = db;
  }

  public void generateAlerts() throws IOException, URISyntaxException {

    long startWindow = System.currentTimeMillis() - 7 * 24 * 60 * 60 * 1000;
    LOG.info("Fetching executions to attempts since " + startWindow);

    Multimap<WorkflowExecution, WorkflowAttempt> attempts = WorkflowQueries.getExecutionsToAttempts(db, null, null, null, null, startWindow, null, null, null);
    LOG.info("Found " + attempts.keySet().size() + " executions");


    for (ExecutionAlertGenerator executionAlert : executionAlerts) {
      Class<? extends ExecutionAlertGenerator> alertClass = executionAlert.getClass();
      LOG.info("Running alert generator " + alertClass);

      for (WorkflowExecution execution : attempts.keySet()) {
        long executionId = execution.getId();

        if (!sentProdAlerts.containsEntry(executionId, alertClass)) {

          for (AlertMessage alertMessage : executionAlert.generateAlerts(execution, attempts.get(execution))) {
            sendAlert(alertClass, execution, alertMessage);
          }
        } else {
          LOG.info("Not re-notifying about execution " + executionId + " alert gen " + alertClass);
        }
      }
    }

    Map<Long, WorkflowExecution> executionsById = getExecutionsById(attempts.keySet());
    NestedMultimap<Long, MapreduceJob, MapreduceCounter> countersPerJob = WorkflowQueries.getCountersByMapreduceJobByExecution(db, startWindow, null);

    for (Long executionId : countersPerJob.k1Set()) {
      WorkflowExecution execution = executionsById.get(executionId);
      Multimap<MapreduceJob, MapreduceCounter> jobToCounters = countersPerJob.get(executionId);

      for (MapreduceJob mapreduceJob : jobToCounters.keySet()) {
        TwoNestedMap<String, String, Long> counterMap = countersAsMap(countersPerJob.get(executionId).get(mapreduceJob));

        for (MapreduceJobAlertGenerator jobAlert : jobAlerts) {
          Class<? extends MapreduceJobAlertGenerator> alertClass = jobAlert.getClass();

          if (!sentProdAlerts.containsEntry(executionId, alertClass)) {
            for (AlertMessage message : jobAlert.generateAlerts(mapreduceJob, counterMap)) {
              sendAlert(alertClass, execution, message);
            }
          } else {
            LOG.info("Not re-notifying about execution " + executionId + " alert gen " + alertClass);
          }
        }
      }
    }
  }

  private void sendAlert(Class alertClass, WorkflowExecution execution, AlertMessage alertMessage) throws IOException, URISyntaxException {
    LOG.info("Sending alert: " + alertMessage + " type " + alertClass + " for execution " + execution);
    long executionId = execution.getId();

    WorkflowRunnerNotification notification = alertMessage.getNotification();
    for (AlertsHandler handler : generator.getRecipients(notification, execution)) {

      AlertMessages.Builder builder = AlertMessages.builder(buildSubject(alertClass.getSimpleName(), execution))
          .setBody(buildMessage(alertMessage.getMesasage(), execution))
          .addToDefaultTags(WorkflowConstants.WORKFLOW_EMAIL_SUBJECT_TAG);

      if (notification.serverity() == AlertSeverity.ERROR) {
        builder.addToDefaultTags(WorkflowConstants.ERROR_EMAIL_SUBJECT_TAG);
      }

      handler.sendAlert(
          builder.build(),
          AlertRecipients.engineering(notification.serverity())
      );
      sentProdAlerts.put(executionId, alertClass);
    }
  }

  private TwoNestedMap<String, String, Long> countersAsMap(Collection<MapreduceCounter> counters) {
    TwoNestedMap<String, String, Long> asMap = new TwoNestedMap<>();
    for (MapreduceCounter counter : counters) {
      asMap.put(counter.getGroup(), counter.getName(), counter.getValue());
    }
    return asMap;
  }

  private Map<Long, WorkflowExecution> getExecutionsById(Collection<WorkflowExecution> executions) {
    Map<Long, WorkflowExecution> executionMap = Maps.newHashMap();
    for (WorkflowExecution execution : executions) {
      executionMap.put(execution.getId(), execution);
    }
    return executionMap;
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
