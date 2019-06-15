package com.liveramp.workflow2.workflow_examples.actions;

import com.rapleaf.cascading_ext.workflow2.Action;

//  Functionally this doesn't need to exist -- you could just use WaitBaseAction with the right generics
//  Exists as a separate class just for simplicity in the README
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
