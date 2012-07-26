package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.counters.NestedCounter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ActionOperation {

  public void start();

  public void complete();

  public String getProperty(String propertyName);

  public int getProgress(int maxPct) throws IOException;

  public String getName();

  // TODO: switch this to return tracking links
  public Map<String, String> getSubStepIdToName(int operationIndex);

  public void timeOperation(Step.StepTimer stepTimer, String checkpointToken, List<NestedCounter> nestedCounters);
}
