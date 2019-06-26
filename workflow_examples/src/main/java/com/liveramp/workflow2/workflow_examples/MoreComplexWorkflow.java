package com.liveramp.workflow2.workflow_examples;

import java.io.IOException;

import com.google.common.collect.Sets;

import com.liveramp.workflow2.workflow_examples.actions.FailingAction;
import com.liveramp.workflow2.workflow_examples.actions.WaitAction;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunners;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;

public class MoreComplexWorkflow {

  public static void main(String[] args) throws IOException {

    Step step1 = new Step(new NoOpAction("step1"));

    Step step2 = new Step(new NoOpAction("step2"));

    Step step3 = new Step(new NoOpAction("step3"));

    Step step4 = new Step(new NoOpAction("step4"));

    Step step5 = new Step(new WaitAction("step5", 180_000), step1, step2, step3, step4);

    Step step6 = new Step(new FailingAction("step6", true), step1);

    Step step7 = new Step(new NoOpAction("step7"), step5, step6);

    Step step8 = new Step(new NoOpAction("step8"), step7);

    Step step9 = new Step(new NoOpAction("step9"), step7);

    Step step10 = new Step(new NoOpAction("step10"), step9);


    WorkflowRunners.dbRun(
        com.liveramp.workflow2.workflow_examples.SimpleWorkflow.class.getName(),
        HadoopWorkflowOptions.test().setMaxConcurrentSteps(5),
        dbHadoopWorkflow -> Sets.newHashSet(step8, step10)
    );

  }

}
