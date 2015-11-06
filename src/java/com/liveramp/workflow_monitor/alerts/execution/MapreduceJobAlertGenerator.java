package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;

public abstract class MapreduceJobAlertGenerator {

  private final Multimap<String, String> countersToFetch;
  protected MapreduceJobAlertGenerator(Multimap<String, String> countersToFetch){
    this.countersToFetch = countersToFetch;
  }

  public abstract List<AlertMessage> generateAlerts(MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException;

  public Multimap<String, String> getCountersToFetch() {
    return countersToFetch;
  }
}
