package com.rapleaf.support.workflow2.action;

import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.support.datastore.BucketDataStore;
import com.rapleaf.support.datastore.VersionedBucketDataStore;
import com.rapleaf.support.workflow2.Action;

public class PersistNewVersion extends Action {

  private final BucketDataStore newVersion;
  private final VersionedBucketDataStore store;
  
  public PersistNewVersion(String checkpointToken, BucketDataStore newVersion, VersionedBucketDataStore store) {
    super(checkpointToken);
    
    this.newVersion = newVersion;
    this.store = store;
    
    readsFrom(newVersion);
    writesTo(store);
  }

  @Override
  protected void execute() throws Exception {
    Bucket newVersion = store.getBucketVersionedStore().openNewVersion();
    newVersion.absorb(this.newVersion.getBucket());
    store.getBucketVersionedStore().completeVersion(newVersion);
  }
}
