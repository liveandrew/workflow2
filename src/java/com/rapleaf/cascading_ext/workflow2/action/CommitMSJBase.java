package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CommitMSJBase <RecordType, KeyType extends Comparable> extends Action {
  private final BucketDataStore<RecordType> baseToCommit;
  private final MSJDataStore<KeyType> store;

  public CommitMSJBase(String checkpointToken, BucketDataStore<RecordType> baseToCommit, MSJDataStore<KeyType> store) {
    super(checkpointToken);

    this.baseToCommit = baseToCommit;
    this.store = store;

    readsFrom(baseToCommit);
    writesTo(store);
  }

  @Override
  protected void execute() throws Exception {
    store.commitBase(baseToCommit.getPath());
  }
}
