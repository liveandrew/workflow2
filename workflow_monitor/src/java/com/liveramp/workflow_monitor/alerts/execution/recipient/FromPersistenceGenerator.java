
package com.liveramp.workflow_monitor.alerts.execution.recipient;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.MailBuffer;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.workflow_core.alerting.AlertsHandlerFactory;
import com.liveramp.workflow_db_state.DbPersistence;
import com.liveramp.workflow_db_state.WorkflowQueries;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

public class FromPersistenceGenerator implements RecipientGenerator {

  private final IDatabases dbs;

  public FromPersistenceGenerator(IDatabases dbs) {
    this.dbs = dbs;
  }

  @Override
  public List<AlertsHandler> getRecipients(WorkflowRunnerNotification severity, WorkflowExecution execution) throws IOException {
    return DbPersistence.queryPersistence(WorkflowQueries.getLatestAttempt(execution).getId(),
        dbs.getWorkflowDb()).getRecipients(severity,
        //  TODO sweep, use a better impl than mailerhelper
        (emails, buffer) -> AlertsHandlers.builder(TeamList.NULL)  // won't actually get used
            .setTestMailBuffer(buffer)
            .setEngineeringRecipient(AlertRecipients.of(emails)).build());
  }
}