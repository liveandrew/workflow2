package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.consolidator.Consolidator;
import com.rapleaf.cascading_ext.datastore.SplitBucketDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

import java.io.IOException;

public class ConsolidateSplitBucketAction extends Action {

  private final SplitBucketDataStore splitBucketDS;
  private final long maxSizeBytes;
  private final int maxWorkers;

  public ConsolidateSplitBucketAction(String checkpointToken, SplitBucketDataStore splitBucketDS)
      throws IOException {
    this(checkpointToken, splitBucketDS, Consolidator.CONSOLIDATION_TARGET_SIZE_BYTES,
      Consolidator.CONSOLIDATION_WORKERS);
  }

  public ConsolidateSplitBucketAction(String checkpointToken,
      SplitBucketDataStore splitBucketDS,
      long maxSizeBytes,
      int maxWorkers) throws IOException {
    super(checkpointToken);
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
