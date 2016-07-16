package com.liveramp.workflow_state;

import java.io.IOException;

import com.liveramp.workflow.types.StepStatus;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

public class Assertions {

  public static void assertCanRevert(IRlDb rldb, WorkflowExecution execution) throws IOException {
    if(!WorkflowQueries.canRevert(rldb, execution)){
      throw new RuntimeException("Cannot revert steps or cancel execution "+execution.getId()+".  Check status and other executions");
    }
  }

  public static void assertStepCompleted(StepAttempt attempt) {
    if (attempt.getStepStatus() != StepStatus.COMPLETED.ordinal()) {
      throw new RuntimeException("Can only revert completed stepss!");
    }
  }

  public static void assertLive(WorkflowAttempt attempt) throws IOException {
    if (WorkflowQueries.getProcessStatus(attempt, attempt.getWorkflowExecution()) != ProcessStatus.ALIVE) {
      throw new RuntimeException("Invalid operation for current attempt " + attempt);
    }
  }

  public static void assertDead(WorkflowAttempt attempt) throws IOException {

    //  if it's still heartbeating, fail loudly
    if(WorkflowQueries.getProcessStatus(attempt, attempt.getWorkflowExecution()) == ProcessStatus.ALIVE){
      throw new RuntimeException("Cannot start, a previous attempt is still alive! Attempt: "+attempt);
    }

  }

}
