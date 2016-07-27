package com.liveramp.workflow_state;

import java.io.IOException;

//  public methods after a workflow is initialized
public interface InitializedPersistence {

  //  should these belong somewhere else?  only really implemented in db-backed one
  long getExecutionId() throws IOException;
  long getAttemptId() throws IOException;

  public void markWorkflowStopped() throws IOException;

}
