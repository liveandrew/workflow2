package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;

public class Example {
  
  public static class SlightlyLessComplex extends MultiStepAction {
    
    public SlightlyLessComplex(String checkpointToken) {
      super(checkpointToken, steps());
    }
    
    private static Collection<Step> steps() {
      Step s1 = new Step(new PrintAction("1"));
      Step s2 = new Step(new PrintAction("2"), s1);
      Step s3 = new Step(new PrintAction("3"), s2);
      return Arrays.asList(s1, s2, s3);
    }
    
  }
  
  public static class ComplexAction extends MultiStepAction {
    public ComplexAction() {
      super("complex", steps());
    }
    
    private static Collection<Step> steps() {
      Collection<Step> steps = new ArrayList<Step>();
      Step s = new Step(new PrintAction("1"));
      steps.add(s);
      s = new Step(new PrintAction("2"), s);
      steps.add(s);
      steps.add(new Step(new PrintAction("3"), s));
      steps.add(new Step(new SlightlyLessComplex("4"), s));
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
    Step s = new Step(new PrintAction("first"));
    new Step(new PrintAction("not dependent on second"), s);
    s = new Step(new PrintAction("second"), s);
    s = new Step(new ComplexAction(), s);
    s = new Step(new PrintAction("last"), s);
    
    new WorkflowRunner(
        Example.class,
        new DbPersistenceFactory(),
        new ProductionWorkflowOptions().setMaxConcurrentSteps(1),
        new HashSet<Step>(Arrays.asList(s))
    ).run();
  }
}
