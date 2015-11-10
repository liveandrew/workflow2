package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;

import com.google.common.collect.Multimap;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;

public abstract class MapreduceJobAlertGenerator {

  protected static final String TASK_COUNTER_GROUP = "org.apache.hadoop.mapreduce.TaskCounter";
  protected static final String JOB_COUNTER_GROUP = "org.apache.hadoop.mapreduce.JobCounter";

  private final Multimap<String, String> countersToFetch;
  protected MapreduceJobAlertGenerator(Multimap<String, String> countersToFetch){
    this.countersToFetch = countersToFetch;
  }

  public abstract AlertMessage generateAlert(MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException;

  public Multimap<String, String> getCountersToFetch() {
    return countersToFetch;
  }


  public Long get(String group, String name, TwoNestedMap<String, String, Long> counters){
    if(counters.containsKey(group, name)){
      return counters.get(group, name);
    }
    return 0L;
  }
}
