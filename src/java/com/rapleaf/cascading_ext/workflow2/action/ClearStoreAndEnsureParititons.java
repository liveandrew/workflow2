package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.BucketUtil;

public class ClearStoreAndEnsureParititons extends Action {

  private final BucketDataStore store;
  private final int numParititons;

  public ClearStoreAndEnsureParititons(String checkpointToken, BucketDataStore store, int numParititons) {
    super(checkpointToken);
    this.store = store;
    this.numParititons = numParititons;

    if (store instanceof VersionedBucketDataStore) {
      throw new IllegalArgumentException("This procedure is invalid for versioned stores");
    }

    creates(store);
  }

  @Override
  protected void execute() throws Exception {
    BucketUtil.addMissingPartitions(store.getBucket(), numParititons);
  }
}
