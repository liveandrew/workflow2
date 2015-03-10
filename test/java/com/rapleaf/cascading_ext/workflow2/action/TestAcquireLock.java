package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;

import org.junit.Test;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;

public class TestAcquireLock extends CascadingExtTestCase{

  @Test
  public void testExclusion() throws IOException {
    try {
      execute(new MultiLock("locks", getTestRoot() + "/lock", getTestRoot() + "/tmp"));
      org.junit.Assert.fail("Locking twice worked.");
    } catch (RuntimeException e) {
      org.junit.Assert.assertTrue(e.getMessage().contains("Could not acquire lock for:"));
    }
  }

  @Test
  public void testRelease() throws IOException {
    execute(new AcquireAndRelease("releasing", getTestRoot() + "/lock", getTestRoot() + "/tmp"));
  }

  private static class MultiLock extends MultiStepAction {

    public MultiLock(String checkpointToken, String tmpRoot, String lockPath) {
      super(checkpointToken, tmpRoot);

      Step lock1 = new Step(new AcquireLock(
          "acquire-lock",
          lockPath
      ));

      Step lock2 = new Step(new AcquireLock(
         "acquire-lock-also",
          lockPath
      ));

      setSubStepsFromTails(lock1, lock2);
    }
  }

  private static class AcquireAndRelease extends MultiStepAction {

    public AcquireAndRelease(String checkpointToken, String tmpRoot, String lockPath) {
      super(checkpointToken, tmpRoot);

      Step lock1 = new Step(new AcquireLock(
          "acquire-lock",
          lockPath
      ));

      Step release = new Step(new ReleaseLock(
          "release-lock",
          lockPath
      ), lock1);

      Step lock2 = new Step(new AcquireLock(
         "acquire-lock-also",
          lockPath
      ), release);

      setSubStepsFromTail(lock2);
    }
  }
}