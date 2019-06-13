package com.liveramp.workflow2.workflow_examples.actions;

import com.liveramp.workflow_core.runner.BaseAction;
import com.rapleaf.cascading_ext.workflow2.Action;

public class NoOpBaseAction extends BaseAction<Void> {

  public NoOpBaseAction(String checkpointToken) {
    super(checkpointToken);
  }

  @Override
  protected void execute() throws Exception {
    // Deliberately do nothing.
  }
}
