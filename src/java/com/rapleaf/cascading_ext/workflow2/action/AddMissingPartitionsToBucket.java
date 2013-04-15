package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.BucketUtil;

public class AddMissingPartitionsToBucket extends Action {
  private final int numPartitions;
  private final BucketDataStore dataStore;

  public AddMissingPartitionsToBucket(String checkpointToken, int numPartitions, BucketDataStore dataStore) {
    super(checkpointToken);
    this.numPartitions = numPartitions;
    this.dataStore = dataStore;

    writesTo(dataStore);
  }

  @Override
  protected void execute() throws Exception {
    dataStore.getBucket().markAsMutable();
    BucketUtil.addMissingPartitions(getFS(), dataStore.getPath(), numPartitions);
    dataStore.getBucket().markAsImmutable();
  }
}
