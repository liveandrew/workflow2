package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.store.MSJDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class PersistNewMSJDelta<RecordType, KeyType extends Comparable> extends Action {

  private final BucketDataStore versionToPersist;
  private final MSJDataStore store;

  public PersistNewMSJDelta(String checkpointToken, BucketDataStore<RecordType> versionToPersist, MSJDataStore<KeyType> store) {
    super(checkpointToken);

    this.versionToPersist = versionToPersist;
    this.store = store;

    readsFrom(versionToPersist);
    writesTo(store);
  }

  @Override
  protected void execute() throws Exception {
    store.commitNewDelta(versionToPersist.getPath());
  }
}
