package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.workflow2.Action;

public class RunnableAction extends Action {

  private final ActionTask task;

  protected interface ActionTask {
    public void run() throws Exception;
  }

  public RunnableAction(String checkpointToken, String tmpDir, ActionTask task) {
    super(checkpointToken, tmpDir);
    this.task = task;
  }

  @Override
  protected void execute() throws Exception {
    task.run();
  }
}
