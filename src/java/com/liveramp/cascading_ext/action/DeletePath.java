package com.liveramp.cascading_ext.action;

import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.rapleaf.cascading_ext.workflow2.Action;

public class DeletePath extends Action {

  private String hdfsPath;

  public DeletePath(
      String checkpointToken,
      String hdfsPath) {
    super(checkpointToken);
    this.hdfsPath = hdfsPath;
  }

  @Override
  protected void execute() throws Exception {
    TrashHelper.deleteUsingTrashIfEnabled(FileSystemHelper.getFileSystemForPath(hdfsPath), new Path(hdfsPath));
  }
}
