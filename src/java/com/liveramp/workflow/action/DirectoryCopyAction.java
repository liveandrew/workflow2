package com.liveramp.workflow.action;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_tools.util.TrackableDirectoryDistCp;
import com.liveramp.java_support.ByteUnit;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.Action;

public class DirectoryCopyAction extends Action {

  private final List<Path> srcPaths;
  private final Path dstPath;
  private final TrackableDirectoryDistCp.TrackedDistCpOptions opts;

  private final long sizeCutoff;

  private static final long DEFAULT_LOCAL_COPY_SIZE_CUTOFF = ByteUnit.GIBIBYTES.toBytes(1);

  public DirectoryCopyAction(String checkpointToken,
                             Path srcPath,
                             Path dstPath) {
    this(checkpointToken, Lists.newArrayList(srcPath), dstPath, new TrackableDirectoryDistCp.TrackedDistCpOptions(), DEFAULT_LOCAL_COPY_SIZE_CUTOFF);
  }

  public DirectoryCopyAction(String checkpointToken,
                             List<Path> srcPaths,
                             Path dstPath) {
    this(checkpointToken, srcPaths, dstPath,  new TrackableDirectoryDistCp.TrackedDistCpOptions(), DEFAULT_LOCAL_COPY_SIZE_CUTOFF);
  }


  public DirectoryCopyAction(String checkpointToken,
                             List<Path> srcPaths,
                             Path dstPath,
                             TrackableDirectoryDistCp.TrackedDistCpOptions opts,
                             long sizeCutoff) {
    super(checkpointToken);

    this.srcPaths = srcPaths;
    this.dstPath = dstPath;
    this.opts = opts;

    this.sizeCutoff = sizeCutoff;

  }

  private long getInputSize(List<Path> inputPaths) throws IOException {
    long size = 0L;
    for (Path inputPath : inputPaths) {
      FileSystem srcFs = FileSystemHelper.getFileSystemForPath(inputPath);
      size += srcFs.getContentSummary(inputPath).getLength();
    }
    return size;
  }

  @Override
  protected void execute() throws Exception {

    long inputSize = getInputSize(srcPaths);
    FileSystem dstFs = FileSystemHelper.getFileSystemForPath(dstPath);

    if (inputSize > sizeCutoff) {

      completeWithProgress(new TrackableDirectoryDistCp(new TrackableDirectoryDistCp.TrackedDistCpConfig(
          srcPaths,
          dstPath,
          getInheritedProperties(),
          opts
      )));

    } else {

      for (Path srcPath : srcPaths) {

        FileSystem srcFs = FileSystemHelper.getFileSystemForPath(srcPath);

        FileUtil.copy(
            srcFs, srcPath,
            dstFs, dstPath,
            false, CascadingHelper.get().getJobConf()
        );

      }

    }

  }


}
