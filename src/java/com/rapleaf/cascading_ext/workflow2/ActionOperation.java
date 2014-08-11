package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.rapleaf.cascading_ext.counters.NestedCounter;

public interface ActionOperation {

  public void start();

  public void complete();

  public String getProperty(String propertyName);

  public int getProgress(int maxPct) throws IOException;

  public String getName();

  public Map<String, String> getSubStepStatusLinks();

  public void timeOperation(Step.StepTimer stepTimer, String checkpointToken, List<NestedCounter> nestedCounters);
}
