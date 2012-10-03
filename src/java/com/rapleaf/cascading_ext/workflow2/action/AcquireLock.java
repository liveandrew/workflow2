package com.rapleaf.cascading_ext.workflow2.action;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rapleaf.cascading_ext.workflow2.Action;
import com.liveramp.cascading_ext.FileSystemHelper;

public class AcquireLock extends Action {
  private final String pathToLock;
  
  public AcquireLock(String checkpointToken, String pathToLock) {
    super(checkpointToken);
    this.pathToLock = pathToLock;
  }
  
  @Override
  protected void execute() throws Exception {
    FileSystem fileSystem = FileSystemHelper.getFS();
    
    if (!fileSystem.createNewFile(new Path(pathToLock))) {
      throw new RuntimeException("Could not acquire lock for: " + pathToLock);
    }
  }
}
