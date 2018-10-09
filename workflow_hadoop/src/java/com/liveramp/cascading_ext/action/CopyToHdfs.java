package com.liveramp.cascading_ext.action;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CopyToHdfs extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(CopyToHdfs.class);

  private final String inputPath;
  private final Path hdfsPath;

  public CopyToHdfs(String checkpointToken, String inputPath, String hdfsPath) {
    super(checkpointToken);
    this.inputPath = inputPath;
    this.hdfsPath = new Path(hdfsPath);
  }

  @Override
  protected void execute() throws Exception {
    setStatusMessage(String.format("Attempting to copy from %s to %s", inputPath, hdfsPath));
    FileSystem fs = FileSystemHelper.getFileSystemForPath(hdfsPath);

    fs.copyFromLocalFile(new Path(inputPath), hdfsPath);

    FileStatus copiedStatus = fs.getFileStatus(hdfsPath);
    if(copiedStatus == null){
      throw new RuntimeException("File does not exist on HDFS!");
    }

    LOG.info("File copied to HDFS: "+copiedStatus.toString());

    setStatusMessage(String.format("Copied from %s to %s", inputPath, hdfsPath));
  }
}
