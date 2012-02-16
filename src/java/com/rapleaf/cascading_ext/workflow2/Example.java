package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

public class Example {
  
  public static class SlightlyLessComplex extends MultiStepAction {
    
    public SlightlyLessComplex(String checkpointToken) {
      super(checkpointToken, steps());
    }
    
    private static Collection<Step> steps() {
      Step s1 = new Step("print 1", new PrintAction("1"));
      Step s2 = new Step("print 2", new PrintAction("2"), s1);
      Step s3 = new Step("print 3", new PrintAction("3"), s2);
      return Arrays.asList(s1, s2, s3);
    }
    
  }
  
  public static class ComplexAction extends MultiStepAction {
    public ComplexAction() {
      super("complex", steps());
    }
    
    private static Collection<Step> steps() {
      Collection<Step> steps = new ArrayList<Step>();
      Step s = new Step("1", new PrintAction("1"));
      steps.add(s);
      s = new Step("2", new PrintAction("2"), s);
      steps.add(s);
      steps.add(new Step("3", new PrintAction("3"), s));
      steps.add(new Step("4", new SlightlyLessComplex("4"), s));
      return steps;
    }
  }
  
  public static class PrintAction extends Action {
    private String message;
    
    public PrintAction(String string) {
      super(string);
      this.message = string;
    }
    
    @Override
    public void execute() {
      System.out.println(message);
    }
  }
  
  public static void main(String[] args) throws IOException {
    Step s = new Step("first", new PrintAction("first"));
    new Step("not dependent on second", new PrintAction("not dependent on second"), s);
    s = new Step("second", new PrintAction("second"), s);
    s = new Step("complex", new ComplexAction(), s);
    s = new Step("last", new PrintAction("last"), s);
    
    new WorkflowRunner("workflow", "/tmp/checkpoint_dir", 2, 12345, new HashSet<Step>(Arrays.asList(s))).run();
  }
}
