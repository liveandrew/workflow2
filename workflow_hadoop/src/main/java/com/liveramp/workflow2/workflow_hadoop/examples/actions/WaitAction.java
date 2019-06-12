package com.liveramp.workflow2.workflow_hadoop.examples.actions;

import com.rapleaf.cascading_ext.workflow2.Action;

public class WaitAction extends Action {

  private final long waitMillis;
  public WaitAction(String checkpointToken, long waitMillis) {
    super(checkpointToken);
    this.waitMillis = waitMillis;
  }

  @Override
  protected void execute() throws Exception {
    Thread.sleep(waitMillis);
  }
}
