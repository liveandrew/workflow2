package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Arrays;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import cascading.tap.Tap;

import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.support.event_timer.TimedEvent;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

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

  @Before
  public void prepare() throws Exception {
    IncrementAction.counter = 0;
  }

  @Test
  public void testSimple() throws Exception {
    Step first = new Step(new IncrementAction("first"));
    Step second = new Step(new IncrementAction("second"), first);

    executeWorkflow(second);

    assertEquals(2, IncrementAction.counter);
  }

  @Test
  public void testWritesCheckpoints() throws Exception {
    Step first = new Step(new IncrementAction("first"));
    Step second = new Step(new FailingAction("second"), first);

    try {
      executeWorkflow(second);
      fail("should have failed!");
    } catch (Exception e) {
      // expected
    }

    assertEquals(1, IncrementAction.counter);
    assertTrue(getFS().exists(new Path(checkpointDir + "/first")));
  }

  @Test
  public void testResume() throws Exception {
    Step first = new Step(new IncrementAction("first"));
    Step second = new Step(new IncrementAction("second"), first);

    getFS().createNewFile(new Path(checkpointDir + "/first"));

    new WorkflowRunner("test", checkpointDir,
        new TestWorkflowOptions(),
        second).run();

    assertEquals(1, IncrementAction.counter);
  }

  @Test
  public void testLoneMultiStepAction() throws Exception {
    // lone multi
    Step s = new Step(new MultiStepAction("lone", Arrays.asList(new Step(
        new IncrementAction("blah")))));

    executeWorkflow(s);

    assertEquals(1, IncrementAction.counter);
  }

  @Test
  public void testMultiInTheMiddle() throws Exception {
    Step s = new Step(new IncrementAction("first"));
    s = new Step(new MultiStepAction("lone", Arrays.asList(new Step(new IncrementAction("blah")))),
        s);
    s = new Step(new IncrementAction("last"), s);

    executeWorkflow(s);

    assertEquals(3, IncrementAction.counter);
  }

  @Test
  public void testMultiAtTheEnd() throws Exception {
    Step s = new Step(new IncrementAction("first"));
    s = new Step(new MultiStepAction("lone", Arrays.asList(new Step(new IncrementAction("blah")))),
        s);

    executeWorkflow(s);

    assertEquals(2, IncrementAction.counter);
  }

  @Test
  public void testMultiInMultiEnd() throws Exception {
    Step s = new Step(new IncrementAction("first"));
    // please, never do this in real code
    s = new Step(new MultiStepAction("depth 1", Arrays.asList(new Step(new MultiStepAction(
        "depth 2", Arrays.asList(new Step(new IncrementAction("blah"))))))), s);
    s = new Step(new IncrementAction("last"), s);

    executeWorkflow(s);

    assertEquals(3, IncrementAction.counter);
  }

  @Test
  public void testMulitInMultiMiddle() throws Exception {
    Step b = new Step(new IncrementAction("b"));
    Step innermost = new Step(new MultiStepAction("innermost", Arrays.asList(new Step(
        new IncrementAction("c")))), b);
    Step d = new Step(new IncrementAction("d"), b);

    Step a = new Step(new IncrementAction("a"));

    Step outer = new Step(new MultiStepAction("outer", Arrays.asList(b, innermost, d)), a);

    executeWorkflow(outer);

    assertEquals(4, IncrementAction.counter);
  }

  @Test
  public void testDuplicateCheckpoints() throws Exception {
    try {
      executeWorkflow(Sets.<Step>newHashSet(
          new Step(new IncrementAction("a")),
          new Step(new IncrementAction("a"))),
          checkpointDir);

      fail("should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testTimingMultiStep() throws Exception {

    Step bottom1 = new Step(new IncrementAction("bottom1"));
    Step bottom2 = new Step(new IncrementAction("bottom2"));

    Step multiMiddle = new Step(new MultiStepAction("middle", Arrays.asList(bottom1, bottom2)));
    Step flatMiddle = new Step(new IncrementAction("flatMiddle"));

    Step top = new Step(new MultiStepAction("Tom's first test dude", Arrays.asList(multiMiddle, flatMiddle)));

    WorkflowRunner testWorkflow = new WorkflowRunner("", checkpointDir,
        new TestWorkflowOptions(),
        top);
    testWorkflow.run();

    assertTrue(testWorkflow.getTimer() != null);

    // Goal here is to detect whether nested MultiTimedEvents ever have "-1"s in their timing and to FAIL if this occurs.

    // Assert that none of the timer.EventStartTime values are -1
    assertTrue(testWorkflow.getTimer().getEventStartTime() != -1);

    TimedEvent middleTimer = multiMiddle.getTimer();
    TimedEvent flatMiddleTimer = flatMiddle.getTimer();

    //    System.out.println("CHILDREN:");
    assertTrue(middleTimer.getEventStartTime() != -1);
    assertTrue(flatMiddleTimer.getEventStartTime() != -1);

    TimedEvent bottom1Timer = bottom1.getTimer();
    TimedEvent bottom2Timer = bottom1.getTimer();

    //    System.out.println("SUBCHILDREN:");
    assertTrue(bottom1Timer.getEventStartTime() != -1);
    assertTrue(bottom2Timer.getEventStartTime() != -1);
  }

  @Test
  public void testSandboxDir() throws Exception {
    try {
      WorkflowRunner wfr = new WorkflowRunner("", checkpointDir,
          new TestWorkflowOptions(),
          fakeStep("a", "/fake/EVIL/../path"),
          fakeStep("b", "/path/of/fakeness"));
      wfr.setSandboxDir("//fake/path");
      wfr.run();
      fail("There was an invalid path!");
    } catch (IOException e) {
      // expected
    }


    try {
      WorkflowRunner wfr = new WorkflowRunner("", checkpointDir,
          new TestWorkflowOptions(),
          fakeStep("a", "/fake/EVIL/../path"),
          fakeStep("b", "/fake/./path"));
      wfr.setSandboxDir("//fake/path");
      wfr.run();
      // expected
    } catch (IOException e) {
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
