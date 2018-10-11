package com.liveramp.workflow2.workflow_hadoop;

import java.io.IOException;

import com.liveramp.workflow2.workflow_hadoop.HdfsStorage;

public class TestHdfsStorage extends BaseTestStorage<HdfsStorage.Factory, String> {

  private int currentRoot = 0;

  @Override
  protected HdfsStorage.Factory createStorage() {
    return new HdfsStorage.Factory();
  }

  @Override
  protected String createRoot() throws IOException {
    return getTestRoot() + "/" + (currentRoot++) + "/";
  }
}
