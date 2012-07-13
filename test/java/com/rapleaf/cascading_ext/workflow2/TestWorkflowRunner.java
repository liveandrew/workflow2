package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Arrays;

import cascading.tap.Tap;
import cascading.tap.hadoop.TapIterator;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.DataStoreImpl;
import org.apache.hadoop.fs.Path;

import com.rapleaf.cascading_ext.CascadingExtTestCase;

public class TestWorkflowRunner extends CascadingExtTestCase {

  public static class FailingAction extends Action {
    public FailingAction(String checkpointToken) {
      super(checkpointToken);
    }

    @Override
    public void execute() {
      throw new RuntimeException("failed on purpose");
    }
  }

  public static class IncrementAction extends Action {
    public IncrementAction(String checkpointToken) {
      super(checkpointToken);
    }

    public static int counter = 0;

    @Override
    public void execute() {
      counter++;
    }
  }

  private final String checkpointDir = getTestRoot() + "/checkpoints";

  public void setUp() throws Exception {
    super.setUp();
    IncrementAction.counter = 0;
  }

  public void testSimple() throws Exception {
    Step first = new Step(new IncrementAction("first"));
    Step second = new Step(new IncrementAction("second"), first);
    new WorkflowRunner("test", checkpointDir, 1, null, second).run();

    assertEquals(2, IncrementAction.counter);
  }

  public void testWritesCheckpoints() throws Exception {
    Step first = new Step(new IncrementAction("first"));
    Step second = new Step(new FailingAction("second"), first);

    try {
      new WorkflowRunner("test", checkpointDir, 1, null, second).run();
      fail("should have failed!");
    } catch (Exception e) {
      // expected
    }

    assertEquals(1, IncrementAction.counter);
    assertTrue(getFS().exists(new Path(checkpointDir + "/first")));
  }

  public void testResume() throws Exception {
    Step first = new Step(new IncrementAction("first"));
    Step second = new Step(new IncrementAction("second"), first);

    getFS().createNewFile(new Path(checkpointDir + "/first"));

    new WorkflowRunner("test", checkpointDir, 1, null, second).run();

    assertEquals(1, IncrementAction.counter);
  }

  public void testLoneMultiStepAction() throws Exception {
    // lone multi
    Step s = new Step(new MultiStepAction("lone", Arrays.asList(new Step(
      new IncrementAction("blah")))));
    new WorkflowRunner("", checkpointDir, 1, null, s).run();
    assertEquals(1, IncrementAction.counter);
  }

  public void testMultiInTheMiddle() throws Exception {
    Step s = new Step(new IncrementAction("first"));
    s = new Step(new MultiStepAction("lone", Arrays.asList(new Step(new IncrementAction("blah")))),
      s);
    s = new Step(new IncrementAction("last"), s);
    new WorkflowRunner("", checkpointDir, 1, null, s).run();
    assertEquals(3, IncrementAction.counter);
  }

  public void testMultiAtTheEnd() throws Exception {
    Step s = new Step(new IncrementAction("first"));
    s = new Step(new MultiStepAction("lone", Arrays.asList(new Step(new IncrementAction("blah")))),
      s);
    new WorkflowRunner("", checkpointDir, 1, null, s).run();
    assertEquals(2, IncrementAction.counter);
  }

  public void testMultiInMultiEnd() throws Exception {
    Step s = new Step(new IncrementAction("first"));
    // please, never do this in real code
    s = new Step(new MultiStepAction("depth 1", Arrays.asList(new Step(new MultiStepAction(
      "depth 2", Arrays.asList(new Step(new IncrementAction("blah"))))))), s);
    s = new Step(new IncrementAction("last"), s);
    new WorkflowRunner("", checkpointDir, 1, null, s).run();
    assertEquals(3, IncrementAction.counter);
  }

  public void testMulitInMultiMiddle() throws Exception {
    Step b = new Step(new IncrementAction("b"));
    Step innermost = new Step(new MultiStepAction("innermost", Arrays.asList(new Step(
      new IncrementAction("c")))), b);
    Step d = new Step(new IncrementAction("d"), b);

    Step a = new Step(new IncrementAction("a"));

    Step outer = new Step(new MultiStepAction("outer", Arrays.asList(b, innermost, d)), a);

    new WorkflowRunner("", checkpointDir, 1, null, outer).run();

    assertEquals(4, IncrementAction.counter);
  }

  public void testDuplicateCheckpoints() throws Exception {
    try {
      new WorkflowRunner("", checkpointDir, 1, null, new Step(new IncrementAction("a")), new Step(
          new IncrementAction("a")));
      fail("should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  public void testSandboxDir() throws Exception {
    try {
      WorkflowRunner wfr = new WorkflowRunner("", checkpointDir, 1, null,
          fakeStep("a", "/fake/EVIL/../path"),
          fakeStep("b", "/path/of/fakeness"));
      wfr.setSandboxDir("//fake/path");
      wfr.run();
      fail("There was an invalid path!");
    } catch (RuntimeException e) {
      // expected
    }


    try {
      WorkflowRunner wfr = new WorkflowRunner("", checkpointDir, 1, null,
          fakeStep("a", "/fake/EVIL/../path"),
          fakeStep("b", "/fake/./path"));
      wfr.setSandboxDir("//fake/path");
      wfr.run();
      // expected
    } catch (RuntimeException e) {
      fail(e.getMessage());
    }
  }

  public Step fakeStep(String checkpointToken, final String fakePath) {
    DataStore dataStore = new DataStore() {
      private String path = fakePath;

      @Override
      public String getName() {
        return "fakeDataStore";
      }

      @Override
      public Tap getTap() {
        return null;
      }

      @Override
      public TapIterator getTapIterator() throws IOException {
        return null;
      }

      @Override
      public String getPath() {
        return path;
      }

      @Override
      public String getRelPath() {
        return "." + path;
      }
    };
    Action action = new IncrementAction(checkpointToken);
    action.creates(dataStore);
    action.createsTemporary(dataStore);
    return new Step(action);
  }
}
