package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.stream.RecordOutputStream;

public class CreateEmptyBucket extends Action {
  private final BucketDataStore dataStore;
  private final int numPartitions;
  private final boolean immutable;
  private String filenamePattern = "%s/part-%05d";

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore, int numPartitions, boolean immutable) {
    super(checkpointToken);
    this.dataStore = dataStore;
    this.numPartitions = numPartitions;
    this.immutable = immutable;
  }

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore, boolean immutable) {
    this(checkpointToken, dataStore, 0, immutable);
  }

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore) {
    this(checkpointToken, dataStore, 0, false);
  }

  @Override
  protected void execute() throws Exception {
    String rootPath = dataStore.getPath();
    Bucket bucket = Bucket.create(getFS(), rootPath);

    for (int partNum = 0; partNum < numPartitions; partNum++) {
      RecordOutputStream os = bucket.openWrite(String.format(filenamePattern, rootPath, partNum));
      os.close();
    }

    if (immutable) {
      bucket.markAsImmutable();
    }
  }
}
