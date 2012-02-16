package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;

public class PersistNewVersion extends Action {
  
  private final BucketDataStore newVersion;
  private final VersionedBucketDataStore store;
  
  public PersistNewVersion(
      BucketDataStore newVersion,
      VersionedBucketDataStore store) {
    super();
    
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
