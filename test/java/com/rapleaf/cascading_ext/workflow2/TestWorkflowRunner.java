package com.rapleaf.cascading_ext.workflow2;

import cascading.tap.Tap;
import com.rapleaf.cascading_ext.CascadingExtTestCase;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.support.event_timer.EventTimer;
import com.rapleaf.support.event_timer.MultiTimedEvent;
import com.rapleaf.support.event_timer.TimedEvent;
import com.rapleaf.support.event_timer.TimedEventWithChildren;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;

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


  public void testTimingMultiStep() throws Exception {

    Step bottom1 = new Step(new IncrementAction("bottom1"));
    Step bottom2 = new Step(new IncrementAction("bottom2"));

    Step multiMiddle = new Step(new MultiStepAction("middle", Arrays.asList(bottom1, bottom2)));
    Step flatMiddle = new Step(new IncrementAction("flatMiddle"));

    Step top = new Step(new MultiStepAction("Tom's first test dude", Arrays.asList(multiMiddle, flatMiddle)));

    WorkflowRunner testWorkflow = new WorkflowRunner("", checkpointDir, 1, null, top);

    testWorkflow.run();

    assertTrue( testWorkflow.getTimer() instanceof EventTimer );

    // Goal here is to detect whether nested MultiTimedEvents ever have "-1"s in their timing and to FAIL if this occurs.

    EventTimer timer = testWorkflow.getTimer();

    TimedEventWithChildren topTimer = (TimedEventWithChildren) timer;

    // Assert that none of the timer.EventStartTime values are -1

//    System.out.println("TOP:");
    assertTrue( topTimer.getEventStartTime() != -1);

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
