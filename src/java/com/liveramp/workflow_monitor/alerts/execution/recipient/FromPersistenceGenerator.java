package com.liveramp.workflow_monitor.alerts.execution.recipient;

import java.io.IOException;
import java.util.List;

import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_db_state.DbPersistence;
import com.liveramp.workflow_db_state.WorkflowQueries;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

public class FromPersistenceGenerator implements RecipientGenerator {

  private final IDatabases dbs;
  public FromPersistenceGenerator(IDatabases dbs){
    this.dbs = dbs;
  }

  @Override
  public List<AlertsHandler> getRecipients(WorkflowRunnerNotification severity, WorkflowExecution execution) throws IOException {
    return DbPersistence.queryPersistence(WorkflowQueries.getLatestAttempt(execution).getId(), dbs.getWorkflowDb()).getRecipients(severity);
  }
}
