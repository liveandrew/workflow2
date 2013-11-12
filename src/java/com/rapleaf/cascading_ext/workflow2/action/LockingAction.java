package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.state.HdfsLock;
import com.rapleaf.cascading_ext.workflow2.Action;

import java.util.concurrent.TimeUnit;

public abstract class LockingAction extends Action {

  private final HdfsLock lock;
  private final boolean blockOnAquireLock;
  private final TimeUnit unit;
  private final Integer interval;

  public LockingAction(String checkpointToken, String lockPath) {
    this(checkpointToken, lockPath, false);
  }

  protected LockingAction(String checkpointToken, String lockPath, boolean blockOnAquireLock) {
    this(checkpointToken, lockPath, blockOnAquireLock, TimeUnit.MINUTES, 1);
  }

  protected LockingAction(String checkpointToken, String lockPath, boolean blockOnAquireLock, TimeUnit unit, Integer interval) {
    super(checkpointToken);
    this.lock = new HdfsLock(lockPath);
    this.blockOnAquireLock = blockOnAquireLock;
    this.unit = unit;
    this.interval = interval;
  }

  @Override
  protected void execute() throws Exception {
    if (blockOnAquireLock) {
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
      throw new RuntimeException("Lock for action " + getCheckpointToken() + " is already held.");
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
