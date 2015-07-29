package com.rapleaf.cascading_ext.datastore;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.tap.TapFactory;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.SinkBinding;

public class PartitionAssemblySinkBinding implements SinkBinding {
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
  public List<DataStore> getOutputStores() {
    return Lists.<DataStore>newArrayList(store);
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
