package com.liveramp.workflow2.workflow_examples.actions;

import com.rapleaf.cascading_ext.workflow2.Action;

public class WaitAction extends Action {

  private final long delay;
  public WaitAction(String checkpointToken, long delay) {
    super(checkpointToken);
    this.delay = delay;
  }

  @Override
  protected void execute() throws Exception {
    setStatusMessage("Sleeping for "+delay+" ms");
    Thread.sleep(delay);
  }
}
