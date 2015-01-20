package com.rapleaf.cascading_ext.workflow2.action;

import java.util.concurrent.TimeUnit;

import com.rapleaf.cascading_ext.state.HdfsLock;
import com.rapleaf.cascading_ext.workflow2.Action;

public abstract class LockingAction extends Action {

  private final HdfsLock lock;
  private final boolean blockOnAcquireLock;
  private final TimeUnit unit;
  private final Integer interval;

  public LockingAction(String checkpointToken, String lockPath) {
    this(checkpointToken, lockPath, false);
  }

  protected LockingAction(String checkpointToken, String lockPath, boolean blockOnAcquireLock) {
    this(checkpointToken, lockPath, blockOnAcquireLock, TimeUnit.MINUTES, 1);
  }

  protected LockingAction(String checkpointToken, String lockPath, boolean blockOnAcquireLock, TimeUnit unit, Integer interval) {
    super(checkpointToken);
    this.lock = new HdfsLock(lockPath);
    this.blockOnAcquireLock = blockOnAcquireLock;
    this.unit = unit;
    this.interval = interval;
  }

  @Override
  protected void execute() throws Exception {
    if (blockOnAcquireLock) {
      blockAndExecute();
    } else {
      executeIfUnlocked();
    }
  }

  private void executeIfUnlocked() throws Exception {
    if (lock.lock()) {
      try {
        protectedExecute();
      } finally {
        lock.clear();
      }
    } else {
      throw new RuntimeException("Lock for action " + getActionId().resolve() + " is already held.");
    }
  }

  private void blockAndExecute() throws Exception {
    lock.blockAndAcquireLock(unit, interval);
    try {
      protectedExecute();
    } finally {
      lock.clear();
    }
  }

  protected abstract void protectedExecute() throws Exception;
}
