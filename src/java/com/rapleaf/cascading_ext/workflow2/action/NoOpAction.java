package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.workflow2.Action;

public class NoOpAction extends Action {
  
  public NoOpAction() {
    super();
  }
  
  public NoOpAction(String tmpRoot) {
    super(tmpRoot);
  }
  
  @Override
  protected void execute() throws Exception {
    // Deliberately do nothing.
  }
}
