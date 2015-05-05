package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.rapleaf.cascading_ext.workflow2.MultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowTestCase;

public class TestBlockAndAcquireLock extends WorkflowTestCase {

  @Test
  public void testRelease() throws IOException {
    execute(new AcquireAndRelease("releasing", getTestRoot() + "/lock", getTestRoot() + "/tmp"));
  }

  private static class AcquireAndRelease extends MultiStepAction {

    public AcquireAndRelease(String checkpointToken, String tmpRoot, String lockPath) {
      super(checkpointToken, tmpRoot);

      Step lock1 = new Step(new BlockAndAcquireLock(
          "acquire-lock",
          lockPath,
          TimeUnit.SECONDS,
          1
      ));

      Step release = new Step(new ReleaseLock(
          "release-lock",
          lockPath
      ), lock1);

      Step lock2 = new Step(new BlockAndAcquireLock(
          "acquire-lock-also",
          lockPath,
          TimeUnit.SECONDS,
          1
      ), release);

      setSubStepsFromTail(lock2);
    }
  }
}