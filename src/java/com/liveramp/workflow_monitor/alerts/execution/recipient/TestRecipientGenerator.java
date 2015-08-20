package com.liveramp.workflow_monitor.alerts.execution.recipient;

import java.io.IOException;

import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

public class TestRecipientGenerator implements RecipientGenerator {

  private final String email;

  public TestRecipientGenerator(String email){
    this.email = email;
  }

  @Override
  public String getRecipient(AlertSeverity severity, WorkflowExecution execution) throws IOException {
    return email;
  }
}
