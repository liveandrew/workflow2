package com.liveramp.workflow.action;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_tools.util.TrackableDirectoryDistCp;
import com.liveramp.java_support.ByteUnit;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.Action;

public class DirectoryCopyAction extends Action {

  private final Path srcPath;
  private final Path dstPath;

  private final long sizeCutoff;

  private static final long DEFAULT_LOCAL_COPY_SIZE_CUTOFF = ByteUnit.GIBIBYTES.toBytes(1);

  public DirectoryCopyAction(String checkpointToken,
                             Path srcPath,
                             Path dstPath) {
    this(checkpointToken, srcPath, dstPath, DEFAULT_LOCAL_COPY_SIZE_CUTOFF);
  }


  public DirectoryCopyAction(String checkpointToken,
                             Path srcPath,
                             Path dstPath,
                             long sizeCutoff) {
    super(checkpointToken);

    this.srcPath = srcPath;
    this.dstPath = dstPath;

    this.sizeCutoff = sizeCutoff;

  }

  @Override
  protected void execute() throws Exception {

    FileSystem srcFs = FileSystemHelper.getFileSystemForPath(srcPath);
    FileSystem dstFs = FileSystemHelper.getFileSystemForPath(dstPath);

    long inputSize = srcFs.getContentSummary(srcPath).getLength();

    if (inputSize > sizeCutoff) {

      completeWithProgress(new TrackableDirectoryDistCp(new TrackableDirectoryDistCp.TrackedDistCpOptions(
          srcPath,
          dstPath,
          getInheritedProperties()
      )));

    } else {

      FileUtil.copy(
          srcFs, srcPath,
          dstFs, dstPath,
          false, CascadingHelper.get().getJobConf()
      );

    }

  }


}
