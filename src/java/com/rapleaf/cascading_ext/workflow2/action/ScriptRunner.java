package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;

import java.io.IOException;

public class ScriptRunner {
  public static void run(String path, CascadingAction action) throws IOException {
    Step step = new Step(action);
    new WorkflowRunner("Oneoff runner: "+action.getClass().getSimpleName(), path+"/checkpoints", 1, 0, step)
        .run();
  }
}
