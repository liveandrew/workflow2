package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;


public class PersistNewVersion<T> extends Action {

  private final BucketDataStore versionToPersist;
  private final VersionedBucketDataStore store;

  public PersistNewVersion(String checkpointToken, BucketDataStore<T> versionToPersist, VersionedBucketDataStore<T> store) {
    super(checkpointToken);

    this.versionToPersist = versionToPersist;
    this.store = store;

    readsFrom(versionToPersist);
    writesTo(store);
  }

  @Override
  protected void execute() throws Exception {
    Bucket newVersion = store.getBucketVersionedStore().openNewVersion();
    newVersion.absorb(versionToPersist.getBucket());

    if (versionToPersist.getBucket().isImmutable()) {
      newVersion.markAsImmutable();
    }

    store.getBucketVersionedStore().completeVersion(newVersion);
  }
}
