package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;

public class TestAction extends CascadingExtTestCase {
  public class ExampleAction extends Action {
    public ExampleAction() throws IOException {
      super("example");
      creates(new BucketDataStoreImpl(getFS(), "example dir 1", getTestRoot(), "/dir1"));
      createsTemporary(new BucketDataStoreImpl(getFS(), "example dir 2", getTestRoot(), "/dir2"));
      readsFrom(new BucketDataStoreImpl(getFS(), "example dir 3", getTestRoot(), "/dir3"));
    }

    @Override
    protected void execute() throws Exception {
    }
  }

  public void testDeletesCreatesAndTemp() throws Exception {
    Path dir1Path = new Path(getTestRoot() + "/dir1");
    getFS().mkdirs(dir1Path);
    Path dir2Path = new Path(getTestRoot() + "/dir2");
    getFS().mkdirs(dir2Path);
    Path dir3Path = new Path(getTestRoot() + "/dir3");
    getFS().mkdirs(dir3Path);

    new ExampleAction().internalExecute();

    assertFalse("dir1 shouldn't exist", getFS().exists(dir1Path));
    assertFalse("dir2 shouldn't exist", getFS().exists(dir2Path));
    assertTrue("dir3 should exist", getFS().exists(dir3Path));
  }
}
