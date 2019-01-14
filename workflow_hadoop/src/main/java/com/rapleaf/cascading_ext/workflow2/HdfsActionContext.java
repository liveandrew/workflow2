package com.rapleaf.cascading_ext.workflow2;

import com.liveramp.cascading_ext.FileSystemHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HdfsActionContext {

  private final String tmpRoot;
  private FileSystem fs;

  public HdfsActionContext(String parentRoot, String checkpointToken) {

    if (parentRoot != null) {
      this.tmpRoot = parentRoot + "/" + checkpointToken + "-tmp-stores";
    } else {
      this.tmpRoot = null;
    }

  }

  public final String getTmpRoot() {
    if (tmpRoot == null) {
      throw new RuntimeException("Temp root not set for action " + this.toString());
    }
    return tmpRoot;
  }

  public FileSystem getFS() throws IOException {
    if (fs == null) {
      fs = FileSystemHelper.getFileSystemForPath(tmpRoot, new Configuration());
    }

    return fs;
  }

}
