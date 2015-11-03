package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;
import java.util.List;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;

public interface MapreduceJobAlertGenerator {
  public List<AlertMessage> generateAlerts(MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException;
}
