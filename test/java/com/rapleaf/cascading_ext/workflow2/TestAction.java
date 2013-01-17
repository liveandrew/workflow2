package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TestAction extends CascadingExtTestCase {
  public class ExampleAction extends Action {
    public ExampleAction() throws IOException {
      super("example");
      creates(new BucketDataStoreImpl(getFS(), "example dir 1", "/data", "/dir1"));
      createsTemporary(new BucketDataStoreImpl(getFS(), "example dir 2", getTestRoot(), "/dir2"));
      readsFrom(new BucketDataStoreImpl(getFS(), "example dir 3", getTestRoot(), "/dir3"));
    }

    @Override
    protected void execute() throws Exception {
    }
  }

  public void testDeletesCreatesAndTemp() throws Exception {
    Path dir1Path = new Path("/data/dir1");
    getFS().mkdirs(dir1Path);
    Path dir2Path = new Path(getTestRoot() + "/dir2");
    getFS().mkdirs(dir2Path);
    Path dir3Path = new Path(getTestRoot() + "/dir3");
    getFS().mkdirs(dir3Path);

    new ExampleAction().internalExecute();
    assertFalse("dir1 should be trashed", getFS().exists(dir1Path));
    assertFalse("dir2 should be deleted", getFS().exists(dir2Path));

    Path trash = new Path(".Trash/Current"+getTestRoot()+"/dir2");

    assertTrue("dir2 should be in trash", getFS().exists(trash));
    assertTrue("dir3 should exist", getFS().exists(dir3Path));
  }
}
