package com.rapleaf.cascading_ext.workflow2.action;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.VersionedBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.stream.RecordOutputStream;

import java.io.IOException;

public class CreateEmptyBucket extends Action {
  private final BucketDataStore dataStore;
  private final int numPartitions;
  private final boolean immutable;
  private final boolean createNewVersion;

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore, int numPartitions, boolean immutable, boolean createNewVersion) {
    super(checkpointToken);
    this.dataStore = dataStore;
    this.numPartitions = numPartitions;
    this.immutable = immutable;
    this.createNewVersion = createNewVersion;


    creates(dataStore);
  }

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore, boolean immutable) {
    this(checkpointToken, dataStore, 0, immutable, false);
  }

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore) {
    this(checkpointToken, dataStore, 0, false, false);
  }

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore, int numPartitions, boolean immutable) {
    this(checkpointToken, dataStore, numPartitions, immutable, false);
  }

  @Override
  protected void execute() throws Exception {
    createEmptyBucket(dataStore, numPartitions, immutable, createNewVersion);
  }

  public static void createEmptyBucket(BucketDataStore dataStore, int numPartitions, boolean immutable, boolean createNewVersion) throws IOException {
    String rootPath = getPath(dataStore, createNewVersion);
    createStore(numPartitions, immutable, rootPath);
  }

  private static String getPath(BucketDataStore dataStore, boolean createNewVersion) throws IOException {
    if (createNewVersion) {
      if (dataStore instanceof VersionedBucketDataStore) {
        return dataStore.getPath() + "/" + System.currentTimeMillis();
      } else {
        throw new IllegalStateException("Cannot create a new version in a non versioned store");
      }
    } else {
      return dataStore.getPath();
    }
  }

  private static void createStore(int numPartitions, boolean immutable, String rootPath) throws IOException {
    Bucket bucket = Bucket.create(FileSystemHelper.getFS(), rootPath);
    String filenamePattern = "%s/part-%05d";

    for (int partNum = 0; partNum < numPartitions; partNum++) {
      RecordOutputStream os = bucket.openWrite(String.format(filenamePattern, rootPath, partNum));
      os.close();
    }

    if (immutable) {
      bucket.markAsImmutable();
    }
  }
}
