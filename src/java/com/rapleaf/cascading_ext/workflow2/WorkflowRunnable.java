package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

//because workflowRunner is a final class, can't be mocked
// make a workflowRunable for mockery test
public class WorkflowRunnable implements Runnable {
  private final WorkflowRunner runner;

  public WorkflowRunnable(WorkflowRunner runner) {
    this.runner = runner;
  }

  @Override
  public void run() {
    try {
      runner.run();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
