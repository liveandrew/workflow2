package com.liveramp.cascading_ext.action;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CopyToNfs extends Action {
  private final String hdfsOutputPath;
  private final String nfsOutputPath;

  public CopyToNfs(String checkpointToken, String hdfsOutputPath, String nfsOutputPath) {
    super(checkpointToken);
    this.hdfsOutputPath = hdfsOutputPath;
    this.nfsOutputPath = nfsOutputPath;
  }

  @Override
  protected void execute() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localFS = FileSystem.getLocal(conf);
    setStatusMessage(String.format("Clearing local path %s if necessary", nfsOutputPath));
    if (new File(nfsOutputPath).delete()) {
      setStatusMessage(String.format("Cleared local path %s", nfsOutputPath));
    }
    FileSystem fs = FileSystemHelper.getFileSystemForPath(hdfsOutputPath);
    setStatusMessage(String.format("Copying from hdfs path %s to local path %s", hdfsOutputPath, nfsOutputPath));
    FileUtil.copyMerge(fs, new Path(hdfsOutputPath), localFS, new Path(nfsOutputPath), false, conf, "");
    setStatusMessage(String.format("Copied from hdfs path %s to local path %s", hdfsOutputPath, nfsOutputPath));
  }
}
