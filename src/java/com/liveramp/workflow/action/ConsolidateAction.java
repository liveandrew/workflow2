package com.liveramp.workflow.action;

import java.io.IOException;

import org.apache.thrift.TBase;

import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.tap.TapFactory;
import com.rapleaf.cascading_ext.tap.Taps;
import com.rapleaf.cascading_ext.workflow2.CascadingAction2;

public class ConsolidateAction<T extends TBase> extends CascadingAction2 {

  public ConsolidateAction(String checkpointToken, String tmpRoot,
                           final BucketDataStore<T> input,
                           final BucketDataStore<T> output,
                           final long minBucketSize,
                           final long maxBucketSize) {
    super(checkpointToken, tmpRoot);

    Pipe pipe = bindSource("source", input, new TapFactory() {
      @Override
      public Tap createTap() throws IOException {
        return Taps.batchBucket(input.getTap()).setMinSplitSize(minBucketSize).setMaxSplitSize(maxBucketSize);
      }
    });
    complete("consolidate", pipe, output);
  }
}
