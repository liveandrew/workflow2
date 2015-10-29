package com.liveramp.workflow_monitor.alerts.execution.recipient;

import java.io.IOException;
import java.util.List;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

public interface RecipientGenerator {
  List<AlertsHandler> getRecipients(WorkflowRunnerNotification severity, WorkflowExecution execution) throws IOException;
}
