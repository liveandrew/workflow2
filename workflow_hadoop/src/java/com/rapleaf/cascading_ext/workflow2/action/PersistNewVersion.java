package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;


public class PersistNewVersion<T> extends Action {

  private final BucketDataStore versionToPersist;
  private final VersionedBucketDataStore store;

  /**
   * Move {@param versionToPersist} to {@param store} as its new version.
   * <p>
   * Caution: make sure {@param versionToPersist} is no longer needed in any action downstream of this one.
   * Otherwise any dependent action will read from an empty store. About one hour has been spent on debugging
   * issues from this gotcha.
   */
  public PersistNewVersion(String checkpointToken, BucketDataStore<T> versionToPersist, VersionedBucketDataStore<T> store) {
    super(checkpointToken);

    this.versionToPersist = versionToPersist;
    this.store = store;

    consumes(versionToPersist);
    writesTo(store);
  }

  @Override
  protected void execute() throws Exception {
    Bucket newVersion = store.getBucketVersionedStore().openNewVersion();
    newVersion.absorbIntoEmpty(versionToPersist.getBucket());

    store.getBucketVersionedStore().completeVersion(newVersion);
  }
}
