package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

public class CPUUsage implements ExecutionAlertGenerator {

  @Override
  public List<AlertMessage> generateAlerts(WorkflowExecution execution, Collection<WorkflowAttempt> attempts) throws IOException {

    //  TODO implement


    return Lists.newArrayList();
  }

}

