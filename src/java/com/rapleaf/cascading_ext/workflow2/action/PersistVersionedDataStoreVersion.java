package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.VersionedDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class PersistVersionedDataStoreVersion extends Action {

  private final DataStore store;
  private final VersionedDataStore versionedDataStore;

  public PersistVersionedDataStoreVersion(String checkpointToken,
                                          DataStore store,
                                          VersionedDataStore versionedStore) {
    super(checkpointToken);

    this.store = store;
    this.versionedDataStore = versionedStore;
  }

  @Override
  protected void execute() throws Exception {
    versionedDataStore.getVersionedTap().absorbNewVersion(store.getPath());
  }
}
