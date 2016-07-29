package com.liveramp.workflow_core;

//public class WorkflowUtil {
//
//  public static void setCheckpointPrefixes(Set<BaseStep> tailSteps) {
//
//    Set<String> explored = Sets.newHashSet();
//    for (BaseStep tailStep : tailSteps) {
//      setCheckpointPrefixes(tailStep, "", explored);
//    }
//  }
//
//  private static <T> void setCheckpointPrefixes(BaseStep<T> step, String prefix, Set<String> explored) {
//
//    step.getAction().getActionId().setParentPrefix(prefix);
//    String resolved = step.getCheckpointToken();
//
//    if (explored.contains(resolved)) {
//      return;
//    }
//
//    if(step.getAction() instanceof BaseMultiStepAction){
//      BaseMultiStepAction<T> msa = (BaseMultiStepAction) step.getAction();
//
//      for (BaseStep<T> tail : msa.getTailSteps()) {
//        setCheckpointPrefixes(tail, resolved + "__", explored);
//      }
//
//    }
//
//    for (BaseStep dep: step.getDependencies()) {
//      setCheckpointPrefixes(dep, prefix, explored);
//    }
//
//  }
//
//}
