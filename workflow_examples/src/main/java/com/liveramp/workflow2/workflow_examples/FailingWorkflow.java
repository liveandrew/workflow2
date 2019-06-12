package com.liveramp.workflow2.workflow_examples;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;

import com.liveramp.workflow2.workflow_examples.actions.FailingAction;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunners;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;

public class FailingWorkflow {

  public static void main(String[] args) throws IOException {

    Step step1 = new Step(new NoOpAction("step1"));

    Step step2 = new Step(new FailingAction("step2", false), step1);

    WorkflowRunners.dbRun(
        FailingWorkflow.class.getName(),
        HadoopWorkflowOptions.test(),
        dbHadoopWorkflow -> Sets.newHashSet(step2)
    );

  }

}
