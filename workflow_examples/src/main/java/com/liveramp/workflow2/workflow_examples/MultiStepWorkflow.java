package com.liveramp.workflow2.workflow_examples;

import java.io.IOException;

import com.google.common.collect.Sets;

import com.liveramp.workflow2.workflow_examples.actions.SimpleMSA;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunners;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;

public class MultiStepWorkflow {

  public static void main(String[] args) throws IOException {

    Step multiStep = new Step(new SimpleMSA("simple-multistep", "/tmp/dir"));

    Step tailStep = new Step(new NoOpAction("later-step"), multiStep);

    WorkflowRunners.dbRun(
        MultiStepWorkflow.class.getName(),
        HadoopWorkflowOptions.test(),
        dbHadoopWorkflow -> Sets.newHashSet(tailStep)
    );

  }

}
