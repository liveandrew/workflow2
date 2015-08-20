package com.liveramp.workflow_monitor.alerts.execution.recipient;

import java.io.IOException;

import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowQueries;

public class FromAttemptGenerator implements RecipientGenerator {
  @Override
  public String getRecipient(AlertSeverity severity, WorkflowExecution execution) throws IOException {
    {
      WorkflowAttempt attempt = WorkflowQueries.getLatestAttempt(execution);
      switch (severity) {
        case ERROR:
          return attempt.getErrorEmail();
        case ONCALL:
          return attempt.getErrorEmail();
        case INFO:
          return attempt.getInfoEmail();
      }
      throw new RuntimeException("Unknown severity: " + severity);
    }
  }
}
