package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.DateVersionedBucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

import java.util.Arrays;

public class CleanUpOlderVersions extends Action {
  private final int numVersionsToKeep;
  private final Iterable<? extends VersionedBucketDataStore> versionedDataStores;

  public CleanUpOlderVersions(String checkpointToken, String tmpRoot, int numVersionsToKeep, VersionedBucketDataStore store) {
    this(checkpointToken, tmpRoot, numVersionsToKeep, Arrays.asList(store));
  }

  public CleanUpOlderVersions(String checkpointToken, String tmpRoot, int numVersionsToKeep, Iterable<? extends VersionedBucketDataStore> versionedDataStores) {
    super(checkpointToken, tmpRoot);

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
