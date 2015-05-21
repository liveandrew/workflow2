package com.rapleaf.cascading_ext.workflow2;

import java.util.List;

import org.apache.hadoop.mapred.RunningJob;

import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.rapleaf.cascading_ext.counters.NestedCounter;

public interface ActionOperation {

  public void complete();

  public String getName();

  public List<RunningJob> listJobs();

  public ThreeNestedMap<String, String, String, Long> getJobCounters() ;

  public interface Complete {
    public void complete(ActionOperation operation);
  }

}
