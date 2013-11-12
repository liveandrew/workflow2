package com.rapleaf.cascading_ext.workflow2.action;

import com.liveramp.cascading_ext.fs.TrashHelper;
import com.rapleaf.cascading_ext.state.HdfsLock;
import org.apache.hadoop.fs.Path;

import com.rapleaf.cascading_ext.workflow2.Action;
import com.liveramp.cascading_ext.FileSystemHelper;

public class ReleaseLock extends Action {
  private final HdfsLock lock;
  
  public ReleaseLock(String checkpointToken, String pathToLock) {
    super(checkpointToken);
    this.lock = new HdfsLock(pathToLock);
  }
  
  @Override
  protected void execute() throws Exception {
    lock.forceClear();
  }
}
