package com.rapleaf.support.workflow2.action;

import java.io.IOException;

import com.rapleaf.formats.bucket.Consolidator;
import com.rapleaf.support.datastore.BucketDataStore;
import com.rapleaf.support.workflow2.Action;

public class ConsolidateAction extends Action {
  
  private final BucketDataStore bucketDS;
  private final long maxSizeBytes;
  private final int maxWorkers;
  
  public ConsolidateAction(String checkpointToken, BucketDataStore bucketDS) throws IOException {
    this(checkpointToken, bucketDS, Consolidator.CONSOLIDATION_TARGET_SIZE_BYTES, Consolidator.CONSOLIDATION_WORKERS);
  }
  
  public ConsolidateAction(String checkpointToken, BucketDataStore bucketDS, long maxSizeBytes, int maxWorkers) throws IOException {
    super(checkpointToken);
    this.bucketDS = bucketDS;
    this.maxSizeBytes = maxSizeBytes;
    this.maxWorkers = maxWorkers;
    readsFrom(bucketDS);
    writesTo(bucketDS);
  }
  
  @Override
  protected void execute() throws IOException {
    bucketDS.getBucket().consolidate(maxSizeBytes, maxWorkers);
  }
  
}
