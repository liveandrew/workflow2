package com.liveramp.cascading_ext.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class ClearAndCreateStore extends Action {

  private final BucketDataStore store;
  public ClearAndCreateStore(String checkpointToken, BucketDataStore store) {
    super(checkpointToken);
    this.store = store;

    creates(store);
  }

  @Override
  protected void execute() throws Exception {
    //  force openOrCreate (create, since we just deleted it)
    store.getBucket();
  }
}
