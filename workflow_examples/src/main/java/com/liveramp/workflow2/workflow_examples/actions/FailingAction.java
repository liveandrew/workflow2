package com.liveramp.workflow2.workflow_examples.actions;

import com.rapleaf.cascading_ext.workflow2.Action;

public class FailingAction extends Action {

  private final boolean fail;

  public FailingAction(String checkpointToken, boolean fail) {
    super(checkpointToken);
    this.fail = fail;
  }

  @Override
  protected void execute() throws Exception {
    if(fail) {
      throw new RuntimeException("Failed on purpose!");
    }
  }

}
