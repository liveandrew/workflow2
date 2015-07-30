package com.liveramp.workflow.msj_store;

import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class AcquireBase<K extends Comparable> extends Action {

  private final MSJDataStore store;

  public AcquireBase(String checkpointToken, MSJDataStore store) {
    super(checkpointToken);
    this.store = store;
  }

  @Override
  protected void execute() throws Exception {
    store.getStore().acquireBaseCreationAttempt();
  }
}
