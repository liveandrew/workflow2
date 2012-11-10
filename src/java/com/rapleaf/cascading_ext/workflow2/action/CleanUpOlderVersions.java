package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.DateVersionedBucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class CleanUpOlderVersions extends Action {
  private final int numVersionsToKeep;
  private final Iterable<VersionedBucketDataStore> versionedDataStores;

  public CleanUpOlderVersions(String checkpointToken, String tmpRoot, int numVersionsToKeep, Iterable<VersionedBucketDataStore> versionedDataStores) {
    super(checkpointToken, tmpRoot);

    if (numVersionsToKeep < 1) {
      throw new RuntimeException("Cannot delete all valid versions of a production data store!");
    }

    this.numVersionsToKeep = numVersionsToKeep;
    this.versionedDataStores = versionedDataStores;
  }

  @Override
  protected void execute() throws Exception {
    for (VersionedBucketDataStore versionedDataStore : versionedDataStores) {
      if (versionedDataStore instanceof DateVersionedBucketDataStore) {
        ((DateVersionedBucketDataStore) versionedDataStore).deleteOlderVersions(numVersionsToKeep);
      } else {
        versionedDataStore.getVersionedStore().deleteOlderVersions(numVersionsToKeep);
      }
    }
  }
}
