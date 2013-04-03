package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.DateVersionedBucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CleanUpOlderVersionedDataStores extends Action {
  private final int numVersionsToKeep;
  private final Iterable<VersionedDataStore> versionedDataStores;

  public CleanUpOlderVersionedDataStores(String checkpointToken, int numVersionsToKeep, Iterable<VersionedDataStore> versionedDataStores) {
    super(checkpointToken);

    if (numVersionsToKeep < 1) {
      throw new RuntimeException("Cannot delete all valid versions of a production data store!");
    }

    this.numVersionsToKeep = numVersionsToKeep;
    this.versionedDataStores = versionedDataStores;
  }

  @Override
  protected void execute() throws Exception {
    for (VersionedDataStore versionedDataStore : versionedDataStores) {
      if (versionedDataStore instanceof DateVersionedBucketDataStore) {
        ((DateVersionedBucketDataStore) versionedDataStore).deleteOlderVersions(numVersionsToKeep);
      } else {
        versionedDataStore.getVersionedStore().deleteOlderVersions(numVersionsToKeep);
      }
    }
  }
}
