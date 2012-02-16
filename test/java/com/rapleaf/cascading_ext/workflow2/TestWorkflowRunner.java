package com.rapleaf.cascading_ext.workflow2;

import java.util.Arrays;

import org.apache.hadoop.fs.Path;

import com.rapleaf.cascading_ext.CascadingExtTestCase;

public class TestWorkflowRunner extends CascadingExtTestCase {
  
  public static class FailingAction extends Action {
    public FailingAction() {
      super();
    }
    
    @Override
    public void execute() {
      throw new RuntimeException("failed on purpose");
    }
  }
  
  public static class IncrementAction extends Action {
    public IncrementAction() {
      super();
    }
    
    public static int counter = 0;
    
    @Override
    public void execute() {
      counter++ ;
    }
  }
  
  private final String checkpointDir = getTestRoot() + "/checkpoints";
  
  public void setUp() throws Exception {
    super.setUp();
    IncrementAction.counter = 0;
  }
  
  public void testSimple() throws Exception {
    Step first = new Step("first", new IncrementAction());
    Step second = new Step("second", new IncrementAction(), first);
    new WorkflowRunner("test", checkpointDir, 1, null, second).run();
    
    assertEquals(2, IncrementAction.counter);
  }
  
  public void testWritesCheckpoints() throws Exception {
    Step first = new Step("first", new IncrementAction());
    Step second = new Step("second", new FailingAction(), first);
    
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
    Step first = new Step("first", new IncrementAction());
    Step second = new Step("second", new IncrementAction(), first);
    
    getFS().createNewFile(new Path(checkpointDir + "/first"));
    
    new WorkflowRunner("test", checkpointDir, 1, null, second).run();
    
    assertEquals(1, IncrementAction.counter);
  }
  
  public void testLoneMultiStepAction() throws Exception {
    // lone multi
    Step s = new Step("lone", new MultiStepAction(Arrays.asList(new Step("blah",
        new IncrementAction()))));
    new WorkflowRunner("", checkpointDir, 1, null, s).run();
    assertEquals(1, IncrementAction.counter);
  }
  
  public void testMultiInTheMiddle() throws Exception {
    Step s = new Step("first", new IncrementAction());
    s = new Step("lone", new MultiStepAction(Arrays.asList(new Step("blah", new IncrementAction()))),
        s);
    s = new Step("last", new IncrementAction(), s);
    new WorkflowRunner("", checkpointDir, 1, null, s).run();
    assertEquals(3, IncrementAction.counter);
  }
  
  public void testMultiAtTheEnd() throws Exception {
    Step s = new Step("last", new IncrementAction());
    s = new Step("lone", new MultiStepAction(Arrays.asList(new Step("blah", new IncrementAction()))),
        s);
    new WorkflowRunner("", checkpointDir, 1, null, s).run();
    assertEquals(2, IncrementAction.counter);
  }
  
  public void testMultiInMultiEnd() throws Exception {
    Step s = new Step("first", new IncrementAction());
    // please, never do this in real code
    s = new Step("depth 1", new MultiStepAction(Arrays.asList(new Step("depth 2", new MultiStepAction(
        Arrays.asList(new Step("blah", new IncrementAction())))))), s);
    s = new Step("last", new IncrementAction(), s);
    new WorkflowRunner("", checkpointDir, 1, null, s).run();
    assertEquals(3, IncrementAction.counter);
  }
  
  public void testMulitInMultiMiddle() throws Exception {
    Step b = new Step("b", new IncrementAction());
    Step innermost = new Step("innermost", new MultiStepAction(Arrays.asList(new Step("c",
        new IncrementAction()))), b);
    Step d = new Step("d", new IncrementAction(), b);
    
    Step a = new Step("a", new IncrementAction());
    
    Step outer = new Step("outer", new MultiStepAction(Arrays.asList(b, innermost, d)), a);
    
    new WorkflowRunner("", checkpointDir, 1, null, outer).run();
    
    assertEquals(4, IncrementAction.counter);
  }
  
   public void testDuplicateCheckpoints() throws Exception {
   try {
   new WorkflowRunner("", checkpointDir, 1, null, new Step("a", new IncrementAction()), new Step("a",
   new IncrementAction()));
   fail("should have thrown an exception");
   } catch (IllegalArgumentException e) {
   // expected
   }
   }
}
