package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;

import com.liveramp.databases.workflow_db.DatabasesImpl;

import static org.junit.Assert.fail;

public class WorkflowTestCase  {

  protected final String TEST_ROOT;

  public WorkflowTestCase() {
    TEST_ROOT = "/tmp/tests/" + "/" + this.getClass().getName() + "_AUTOGEN";
  }

  @Before
  public void deleteFixtures() throws Exception {
    new DatabasesImpl().getWorkflowDb().deleteAll();
    new com.liveramp.databases.workflow_db.DatabasesImpl().getWorkflowDb().deleteAll();
  }

  public String getTestRoot() {
    return TEST_ROOT;
  }

  protected Exception getException(Callable run) {
    try {
      run.call();
      fail("Should have thrown an exception!");
      throw new RuntimeException("won't get here");
    } catch (Exception e) {
      return e;
    }
  }

}
