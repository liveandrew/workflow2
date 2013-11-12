package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.state.HdfsLock;
import com.rapleaf.cascading_ext.workflow2.Action;

public abstract class LockingAction extends Action {

  private final HdfsLock lock;

  public LockingAction(String checkpointToken, String lockPath) {
    super(checkpointToken);
    this.lock = new HdfsLock(lockPath + "/" + checkpointToken);
  }

  @Override
  protected void execute() throws Exception {
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

  protected abstract void protectedExecute() throws Exception;
}
