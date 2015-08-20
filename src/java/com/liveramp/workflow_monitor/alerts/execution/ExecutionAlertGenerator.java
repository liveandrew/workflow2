package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;
import java.util.List;

import com.rapleaf.db_schemas.IDatabases;

public interface ExecutionAlertGenerator {
  public List<ExecutionAlert> generateAlerts(IDatabases db) throws IOException;
}
