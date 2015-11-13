package com.liveramp.workflow_state;

public enum WorkflowExecutionStatus {
  INCOMPLETE,
  COMPLETE,
  CANCELLED;

  public static WorkflowExecutionStatus findByValue(int val){
    return values()[val];
  }

}
