package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.msj_tap.compaction.Compactor;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class MSJDataStoreCompactionAction extends Action {

  private final MSJDataStore store;

  public MSJDataStoreCompactionAction(MSJDataStore store) {
    super("compact_" + store.getPath());
    this.store = store;
  }

  @Override
  protected void execute() throws Exception {
    Compactor compactor = new Compactor();
    compactor.add(store.getCompactionTask());
    compactor.run();
  }
}
