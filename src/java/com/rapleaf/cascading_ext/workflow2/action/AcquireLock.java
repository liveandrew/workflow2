package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.state.HdfsLock;
import com.rapleaf.cascading_ext.workflow2.Action;

public class AcquireLock extends Action {
  private final HdfsLock lock;

  public AcquireLock(String checkpointToken, String pathToLock) {
    super(checkpointToken);
    this.lock = new HdfsLock(pathToLock);
  }

  @Override
  protected void execute() throws Exception {
    if (lock.lock()) {
      throw new RuntimeException("Could not acquire lock for: " + lock);
    }
  }
}
