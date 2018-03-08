package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.java_support.ByteUnit;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.formats.bucket.Bucket;
import com.rapleaf.formats.bucket.BucketUtil;

public class AddMissingPartitionsToBucket<T> extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(AddMissingPartitionsToBucket.class);
  private static final long MAX_TEMP_FILE_BYTE_SIZE = ByteUnit.BYTES.toBytes(200);

  protected final int numPartitions;
  protected final BucketDataStore<T> dataStore;
  protected final Class<T> recordClass;
  private final boolean removeTempFiles;

  public AddMissingPartitionsToBucket(String checkpointToken, int numPartitions, BucketDataStore<T> dataStore, Class<T> recordClass) {
    this(checkpointToken, numPartitions, dataStore, recordClass, false);
  }

  public AddMissingPartitionsToBucket(String checkpointToken, int numPartitions, BucketDataStore<T> dataStore, Class<T> recordClass, boolean removeTempFiles) {
    super(checkpointToken);
    this.numPartitions = numPartitions;
    this.dataStore = dataStore;
    this.recordClass = recordClass;
    this.removeTempFiles = removeTempFiles;

    writesTo(dataStore);
  }

  @Override
  protected void execute() throws Exception {
    dataStore.getBucket().markAsMutable();

    // Remove temp files to make this action resume-friendly
    if (removeTempFiles) {
      removeTempFiles();
    }

    BucketUtil.addMissingPartitions(
        FileSystemHelper.getFileSystemForPath(dataStore.getPath()),
        dataStore.getPath(), numPartitions, recordClass
    );
    dataStore.getBucket().markAsImmutable();
  }

  private void removeTempFiles() throws IOException {
    String storePath = dataStore.getPath();
    FileSystem fs = FileSystemHelper.getFileSystemForPath(storePath);
    for (final FileStatus fileStatus : FileSystemHelper.safeListStatus(fs, new Path(storePath))) {
      Path filePath = fileStatus.getPath();

      // Deleting larger temp files could mask cases where the original production of the store was corrupt (i.e lingering bucketfile_in_progress tmp files)
      if (fileStatus.isFile()
          && filePath.getName().endsWith(Bucket.TEMP_FILE_SUFFIX)
          && fileStatus.getLen() < MAX_TEMP_FILE_BYTE_SIZE) {
        TrashHelper.deleteUsingTrashIfEnabled(fs, filePath);
        LOG.info("Removed temp file {}", filePath.getName());
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    String path = args[0];
    Class clazz = Class.forName(args[1]);
    int numPartitions = args.length > 2 ? Integer.parseInt(args[2]) : 599;
    BucketDataStore dataStore = new BucketDataStoreImpl(FileSystemHelper.getFS(), "bucket", path, "", clazz);
    new WorkflowRunner(
        AddMissingPartitionsToBucket.class,
        new Step(new AddMissingPartitionsToBucket("checkpoint", numPartitions, dataStore, clazz))
    ).run();
  }
}
