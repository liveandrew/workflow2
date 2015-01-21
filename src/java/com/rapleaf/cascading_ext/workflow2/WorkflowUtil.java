package com.rapleaf.cascading_ext.workflow2;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import com.rapleaf.cascading_ext.workflow2.state.StepState;
import com.rapleaf.cascading_ext.workflow2.state.StepStatus;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowStatePersistence;

public class WorkflowUtil {

  public static void setCheckpointPrefixes(Set<Step> tailSteps) {

    Set<String> explored = Sets.newHashSet();
    for (Step tailStep : tailSteps) {
      setCheckpointPrefixes(tailStep, "", explored);
    }
  }

  private static void setCheckpointPrefixes(Step step, String prefix, Set<String> explored) {

    step.getAction().getActionId().setParentPrefix(prefix);
    String resolved = step.getCheckpointToken();

    if (explored.contains(resolved)) {
      return;
    }

    if(step.getAction() instanceof MultiStepAction){
      MultiStepAction msa = (MultiStepAction) step.getAction();

      for (Step tail : msa.getTailSteps()) {
        setCheckpointPrefixes(tail, resolved + "__", explored);
      }

    }

    for (Step dep: step.getDependencies()) {
      setCheckpointPrefixes(dep, prefix, explored);
    }

  }

  public static boolean isShutdownPending(WorkflowStatePersistence persistence) {
    return persistence.getShutdownRequest() != null;
  }

  public static boolean isFailPending(WorkflowStatePersistence persistence) {

    for (Map.Entry<String, StepState> entry : persistence.getStepStatuses().entrySet()) {
      if (entry.getValue().getStatus() == StepStatus.FAILED) {
        return true;
      }
    }

    return false;
  }
}
