package com.liveramp.workflow.action;

import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_tools.util.TrackableDirectoryDistCp;
import com.rapleaf.cascading_ext.workflow2.Action;

public class DirectoryCopyAction extends Action {

  private final Path srcPath;
  private final Path dstPath;

  public DirectoryCopyAction(String checkpointToken,
                             Path srcPath,
                             Path dstPath) {
    super(checkpointToken);

    this.srcPath = srcPath;
    this.dstPath = dstPath;

  }

  @Override
  protected void execute() throws Exception {
    completeWithProgress(new TrackableDirectoryDistCp(new TrackableDirectoryDistCp.TrackedDistCpOptions(
        srcPath,
        dstPath,
        getInheritedProperties()
    )));
  }
}
