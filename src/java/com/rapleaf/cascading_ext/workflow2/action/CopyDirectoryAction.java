package com.rapleaf.cascading_ext.workflow2.action;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_tools.util.DirectoryDistCp;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CopyDirectoryAction extends Action {
  private final DirectoryDistCp distCpCopier;
  private final Path srcDir;
  private final Path dstDir;

  public CopyDirectoryAction(final String checkpointToken,
                             final String tmpDir,
                             final Path srcDir,
                             final Path dstDir) {
    super(checkpointToken, tmpDir);
    this.distCpCopier = new DirectoryDistCp();
    this.srcDir = Preconditions.checkNotNull(srcDir, "Source directory can't be null");
    this.dstDir = Preconditions.checkNotNull(dstDir, "Destination directory can't be null");
  }

  @Override
  protected void execute() throws Exception {
    distCpCopier.copyDirectory(srcDir, dstDir);
  }
}
