package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlert;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowQueries;

public class DiedUnclean implements ExecutionAlertGenerator {

  @Override
  public List<ExecutionAlert> generateAlerts(IDatabases db) throws IOException {

    List<ExecutionAlert> alerts = Lists.newArrayList();

    for (WorkflowExecution execution : WorkflowQueries.getDiedUncleanExecutions(db, null, 7)) {
      long id = execution.getId();

      alerts.add(new ExecutionAlert(id,
          "Execution has died unclean.  Please cancel or resume.",
          AlertSeverity.ERROR)
      );

    }

    return alerts;
  }

}
