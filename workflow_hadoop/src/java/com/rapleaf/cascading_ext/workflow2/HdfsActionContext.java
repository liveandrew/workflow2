package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;

public class HdfsActionContext {

  private final String tmpRoot;
  private final DataStoreBuilder builder;
  private FileSystem fs;

  public HdfsActionContext(String parentRoot, String checkpointToken) {

    if (parentRoot != null) {
      this.tmpRoot = parentRoot + "/" + checkpointToken + "-tmp-stores";
      this.builder = new DataStoreBuilder(getTmpRoot());
    } else {
      this.tmpRoot = null;
      this.builder = null;
    }

  }

  public final String getTmpRoot() {
    if (tmpRoot == null) {
      throw new RuntimeException("Temp root not set for action " + this.toString());
    }
    return tmpRoot;
  }

  protected FileSystem getFS() throws IOException {
    if (fs == null) {
      fs = FileSystemHelper.getFS();
    }

    return fs;
  }

  public DataStoreBuilder getBuilder() {
    return builder;
  }
}
