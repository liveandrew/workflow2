package com.liveramp.cascading_ext.action;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.rapleaf.cascading_ext.workflow2.Action;

public class DeletePath extends Action {

  private final String hdfsPath;
  private final boolean useTrash;

  public DeletePath(
      String checkpointToken,
      String hdfsPath,
      boolean useTrash) {
    super(checkpointToken);
    this.hdfsPath = hdfsPath;
    this.useTrash = useTrash;
  }

  public DeletePath(
      String checkpointToken,
      String hdfsPath) {
    this(checkpointToken, hdfsPath, true);
  }

  @Override
  protected void execute() throws Exception {
    FileSystem fs = FileSystemHelper.getFileSystemForPath(hdfsPath);
    if (useTrash) {
      TrashHelper.deleteUsingTrashIfEnabled(fs, new Path(hdfsPath));
    } else if (hdfsPath.startsWith("/tmp/")) {
      fs.delete(new Path(hdfsPath), true);
    } else {
      throw new RuntimeException("Only tmp paths can be deleted without using the trash. Path " + hdfsPath + " is illegal");
    }
  }
}
