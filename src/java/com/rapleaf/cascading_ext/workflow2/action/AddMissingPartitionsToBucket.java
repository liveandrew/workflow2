package com.rapleaf.cascading_ext.workflow2.action;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.BucketDataStoreImpl;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.formats.bucket.BucketUtil;

public class AddMissingPartitionsToBucket<T> extends Action {
  protected final int numPartitions;
  protected final BucketDataStore<T> dataStore;
  protected final Class<T> recordClass;

  public AddMissingPartitionsToBucket(String checkpointToken, int numPartitions, BucketDataStore<T> dataStore, Class<T> recordClass) {
    super(checkpointToken);
    this.numPartitions = numPartitions;
    this.dataStore = dataStore;
    this.recordClass = recordClass;

    writesTo(dataStore);
  }

  @Override
  protected void execute() throws Exception {
    dataStore.getBucket().markAsMutable();
    BucketUtil.addMissingPartitions(
        FileSystemHelper.getFileSystemForPath(dataStore.getPath()),
        dataStore.getPath(), numPartitions, recordClass
    );
    dataStore.getBucket().markAsImmutable();
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
