package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CommitNewMSJDelta<RecordType, KeyType extends Comparable> extends Action {
  private final BucketDataStore<RecordType> versionToCommit;
  private final MSJDataStore<KeyType> store;

  public CommitNewMSJDelta(String checkpointToken, BucketDataStore<RecordType> versionToCommit, MSJDataStore<KeyType> store) {
    super(checkpointToken);

    this.versionToCommit = versionToCommit;
    this.store = store;

    readsFrom(versionToCommit);
    writesTo(store);
  }

  @Override
  protected void execute() throws Exception {
    store.commitNewDelta(versionToCommit.getPath());
  }
}
