package com.rapleaf.cascading_ext.workflow2.action;

import java.util.concurrent.TimeUnit;

import com.rapleaf.cascading_ext.state.HdfsLock;
import com.rapleaf.cascading_ext.workflow2.Action;

public class BlockAndAcquireLock extends Action {
  private final HdfsLock lock;
  private final TimeUnit timeUnit;
  private final int numberOfUnitsToWait;

  public BlockAndAcquireLock(
      String checkpointToken,
      String pathToLock,
      TimeUnit timeUnit,
      int numberOfUnitsToWait) {
    super(checkpointToken);
    this.lock = new HdfsLock(pathToLock);
    this.timeUnit = timeUnit;
    this.numberOfUnitsToWait = numberOfUnitsToWait;
  }

  @Override
  protected void execute() throws Exception {
    lock.blockAndAcquireLock(timeUnit, numberOfUnitsToWait);
  }
}