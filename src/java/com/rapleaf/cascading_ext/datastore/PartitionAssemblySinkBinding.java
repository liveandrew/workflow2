package com.rapleaf.cascading_ext.datastore;

import java.io.IOException;

import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.SinkBinding;
import com.rapleaf.cascading_ext.tap.TapFactory;

public class PartitionAssemblySinkBinding implements SinkBinding.DataStoreSink, SinkBinding {
  private final PartitionedDataStore store;
  private final Pipe partitionAssembly;
  private final TapFactory tapFactory;

  public PartitionAssemblySinkBinding(final PartitionedDataStore store, Pipe partitionAssembly, final PartitionStructure structure) {
    this.store = store;
    this.partitionAssembly = partitionAssembly;
    this.tapFactory = new TapFactory() {
      @Override
      public Tap createTap() throws IOException {
        return store.getSinkTap(structure);
      }
    };
  }

  @Override
  public DataStore getOutputStore() {
    return store;
  }

  @Override
  public Pipe getPipe() {
    return partitionAssembly;
  }

  @Override
  public TapFactory getTapFactory() {
    return tapFactory;
  }
}
