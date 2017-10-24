package com.liveramp.workflow.action;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.PlaceholderDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;

public class MoveAction<T extends TBase> extends Action {

  private final String tempBucketPath;
  private final String productionBucketPath;
  private final Class<T> recordType;

  public MoveAction(String checkpointToken,
                    Class<T> recordType,
                    String tempBucketPAth,
                    String productionBucketPath) {
    super(checkpointToken);

    this.tempBucketPath = tempBucketPAth;
    this.productionBucketPath = productionBucketPath;
    this.recordType = recordType;

    consumes(PlaceholderDataStore.wrap(new Path(tempBucketPAth)));
    writesTo(PlaceholderDataStore.wrap(new Path(productionBucketPath)));
  }

  @Override
  protected void execute() throws Exception {
    final FileSystem fs = FileSystemHelper.getFileSystemForPath(tempBucketPath);

    final Bucket tempSplitBucket = Bucket.openOrCreate(fs, tempBucketPath, recordType);
    final Bucket productionSplitBucket = Bucket.openOrCreate(fs, productionBucketPath, recordType);

    productionSplitBucket.move(tempSplitBucket);
    setStatusMessage(String.format("Moved %s to %s", tempSplitBucket.getInstanceRoot(), productionSplitBucket.getInstanceRoot()));
  }
}
