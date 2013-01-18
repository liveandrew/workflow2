package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;

import java.io.IOException;

public class ScriptRunner {
  public static void run(CascadingAction action) throws IOException {
    String tmpPath = "/tmp/oneoff-script-runner/"+action.getCheckpointToken() +"/checkpoints";

    Step step = new Step(action);
    new WorkflowRunner("Oneoff runner: "+action.getClass().getSimpleName(), tmpPath, 1, 0, step)
        .run();
  }
}
