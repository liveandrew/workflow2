package com.liveramp.workflow2.workflow_examples.actions;

import com.liveramp.workflow2.workflow_hadoop.HadoopMultiStepAction;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;

public class SimpleMSA extends HadoopMultiStepAction {

  public SimpleMSA(String checkpointToken, String tmpRoot) {
    super(checkpointToken, tmpRoot);

    Step step1 = new Step(new NoOpAction("step1"));

    Step step2 = new Step(new NoOpAction("step2"), step1);

    setSubStepsFromTails(step2);
  }

}
