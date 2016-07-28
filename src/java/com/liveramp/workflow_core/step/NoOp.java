package com.liveramp.workflow_core.step;

import com.liveramp.workflow_core.runner.BaseAction;

public class NoOp extends BaseAction {

  public NoOp(String checkpointToken) {
    super(checkpointToken);
  }

  @Override
  protected void execute() throws Exception {
    //  no op
  }
}
