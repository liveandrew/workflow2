package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.stream.RecordOutputStream;
import com.rapleaf.support.FileSystemHelper;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class CreateEmptyBucket extends Action {
  private final BucketDataStore dataStore;
  private final int numPartitions;
  private final boolean immutable;

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore, int numPartitions, boolean immutable) {
    super(checkpointToken);
    this.dataStore = dataStore;
    this.numPartitions = numPartitions;
    this.immutable = immutable;
    
    creates(dataStore);
  }

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore, boolean immutable) {
    this(checkpointToken, dataStore, 0, immutable);
  }

  public CreateEmptyBucket(String checkpointToken, BucketDataStore dataStore) {
    this(checkpointToken, dataStore, 0, false);
  }

  @Override
  protected void execute() throws Exception {
    createEmptyBucket(dataStore, numPartitions, immutable);
  }

  public static void createEmptyBucket(BucketDataStore dataStore, int numPartitions, boolean immutable) throws IOException {
    String rootPath = dataStore.getPath();
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
