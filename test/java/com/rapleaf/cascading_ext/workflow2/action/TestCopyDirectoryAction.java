package com.rapleaf.cascading_ext.workflow2.action;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;
import com.rapleaf.formats.bucket.Bucket;

public class TestCopyDirectoryAction extends WorkflowTestCase {
  @Test
  public void testBasic() throws Exception {
    final Path srcPath = new Path(getTestRoot() + "/dir1");
    final Path dstPath = new Path(getTestRoot() + "/dir2");

    Bucket.create(getFS(), srcPath.toString(), byte[].class);

    execute(new CopyDirectoryAction("cp", getTestRoot() + "/tmp", srcPath, dstPath));

    Assert.assertTrue("Should copy bucket data", Bucket.exists(getFS(), srcPath.toString()));
  }

  @Test
  public void testExisting() throws Exception {
    final Path srcPath = new Path(getTestRoot() + "/dir1");
    final Path dstPath = new Path(getTestRoot() + "/dir2");

    Bucket.create(getFS(), srcPath.toString(), byte[].class);
    Bucket.create(getFS(), dstPath.toString(), byte[].class);

    execute(new CopyDirectoryAction("cp", getTestRoot() + "/tmp", srcPath, dstPath));

    Assert.assertTrue("Should copy bucket data", Bucket.exists(getFS(), srcPath.toString()));
  }
}