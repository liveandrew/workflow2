package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;

import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Consolidator;

public class ConsolidateSplitBucketAction extends Action {
  
  private final SplitBucketDataStore splitBucketDS;
  private final long maxSizeBytes;
  private final int maxWorkers;
  
  public ConsolidateSplitBucketAction(SplitBucketDataStore splitBucketDS)
    throws IOException {
    this(splitBucketDS, Consolidator.CONSOLIDATION_TARGET_SIZE_BYTES,
        Consolidator.CONSOLIDATION_WORKERS);
  }
  
  public ConsolidateSplitBucketAction(
      SplitBucketDataStore splitBucketDS,
      long maxSizeBytes,
      int maxWorkers) throws IOException {
    super();
    this.splitBucketDS = splitBucketDS;
    this.maxSizeBytes = maxSizeBytes;
    this.maxWorkers = maxWorkers;
    readsFrom(splitBucketDS);
    writesTo(splitBucketDS);
  }
  
  @Override
  protected void execute() throws IOException {
    splitBucketDS.getAttributeBucket().consolidate(maxSizeBytes, maxWorkers);
  }
  
}
