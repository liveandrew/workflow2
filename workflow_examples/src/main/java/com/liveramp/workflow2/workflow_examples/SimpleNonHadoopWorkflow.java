package com.liveramp.workflow2.workflow_examples;

import java.io.IOException;

import com.liveramp.workflow2.workflow_examples.actions.NoOpBaseAction;
import com.liveramp.workflow_core.CoreOptions;
import com.liveramp.workflow_core.runner.BaseStep;
import com.liveramp.workflow_db_state.runner.WorkflowDbRunners;
import com.rapleaf.cascading_ext.workflow2.BaseWorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.Step;

public class SimpleNonHadoopWorkflow {

  public static void main(String[] args) throws IOException {

    BaseStep<Void> step1 = new BaseStep<>(new NoOpBaseAction("step1"));

    BaseStep<Void> step2 = new BaseStep<>(new NoOpBaseAction("step2"), step1);

    WorkflowDbRunners.baseWorkflowDbRunner(
        SimpleNonHadoopWorkflow.class,
        CoreOptions.test(),
        step2
    );

  }

}
