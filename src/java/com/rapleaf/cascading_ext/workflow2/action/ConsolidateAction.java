package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Consolidator;

public class ConsolidateAction extends Action {

  private final BucketDataStore bucketDS;
  private final long maxSizeBytes;
  private final int maxWorkers;

  public ConsolidateAction(String checkpointToken, BucketDataStore bucketDS) throws IOException {
    this(checkpointToken, bucketDS, Consolidator.CONSOLIDATION_TARGET_SIZE_BYTES,
      Consolidator.CONSOLIDATION_WORKERS);
  }

  public ConsolidateAction(String checkpointToken,
      BucketDataStore bucketDS,
      long maxSizeBytes,
      int maxWorkers) throws IOException {
    super(checkpointToken);
    this.bucketDS = bucketDS;
    this.maxSizeBytes = maxSizeBytes;
    this.maxWorkers = maxWorkers;
    
    if(bucketDS instanceof SplitBucketDataStore){
      throw new RuntimeException("split bucket "+bucketDS.getName()+" should not be consolidated with ConsolidateAction! use ConsolidateSplitBucketAction");
    }
    
    readsFrom(bucketDS);
    writesTo(bucketDS);
  }

  @Override
  protected void execute() throws IOException {
    bucketDS.getBucket().consolidate(maxSizeBytes, maxWorkers);
  }

}
