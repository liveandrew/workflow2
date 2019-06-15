package com.liveramp.workflow2.workflow_examples.actions;

import com.liveramp.workflow_core.runner.BaseAction;

public class WaitBaseAction extends BaseAction<Void>  {

  private final long delay;

  public WaitBaseAction(String checkpointToken, long delay) {
    super(checkpointToken);
    this.delay = delay;
  }

  @Override
  protected void execute() throws Exception {
    setStatusMessage("Sleeping for "+delay+" ms");
    Thread.sleep(delay);
  }
}
