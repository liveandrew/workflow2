package com.rapleaf.cascading_ext.workflow2;

import java.util.concurrent.Semaphore;

public class DelayedFailingAction extends Action {
  private final Semaphore semaphore;

  public DelayedFailingAction(String checkpointToken, Semaphore sem) {
    super(checkpointToken);
    this.semaphore = sem;
  }

  @Override
  public void execute() throws InterruptedException {
    semaphore.acquire();
    throw new RuntimeException("failed on purpose");
  }
}
