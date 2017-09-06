package com.rapleaf.cascading_ext.workflow2;

import java.util.Collection;

import com.liveramp.workflow_core.runner.BaseAction;
import com.liveramp.workflow_core.runner.BaseStep;

public final class HadoopStep extends BaseStep<WorkflowRunner.ExecuteConfig> {

  public HadoopStep(BaseAction<WorkflowRunner.ExecuteConfig> action, Step... dependencies) {
    super(action, dependencies);
  }

  public HadoopStep(BaseAction<WorkflowRunner.ExecuteConfig> action, Collection<Step> dependencies) {
    super(action, dependencies);
  }
}
