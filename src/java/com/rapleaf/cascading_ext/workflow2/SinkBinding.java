package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.msj_tap.store.PartitionableDataStore;
import com.rapleaf.cascading_ext.tap.TapFactory;
import com.rapleaf.cascading_ext.tap.TapFactory.NullTapFactory;
import com.rapleaf.cascading_ext.tap.TapFactory.SimpleFactory;

public interface SinkBinding {
  public Pipe getPipe();

  public TapFactory getTapFactory();

  public List<DataStore> getOutputStores();

  public class DSSink implements SinkBinding {
    private final Pipe pipe;
    private final DataStore outputStore;

    public DSSink(Pipe pipe, DataStore outputStore) {
      this.pipe = pipe;
      this.outputStore = outputStore;
    }

    @Override
    public Pipe getPipe() {
      return pipe;
    }

    @Override
    public TapFactory getTapFactory() {
      return new SimpleFactory(outputStore);
    }

    public List<DataStore> getOutputStores() {
      return Lists.newArrayList(outputStore);
    }
  }

  public class FutureTap implements SinkBinding {

    private final Pipe pipe;
    private final TapFactory tap;
    private List<DataStore> dataStores;

    public FutureTap(Pipe pipe, TapFactory tap) {
      this(pipe, tap, Lists.<DataStore>newArrayList());
    }

    public FutureTap(Pipe pipe, TapFactory tap, List<DataStore> dataStores) {
      this.pipe = pipe;
      this.tap = tap;
      this.dataStores = dataStores;
    }

    @Override
    public Pipe getPipe() {
      return pipe;
    }

    @Override
    public TapFactory getTapFactory() {
      return tap;
    }

    @Override
    public List<DataStore> getOutputStores() {
      return dataStores;
    }
  }

  public class RawTap implements SinkBinding {

    private final Pipe pipe;
    private final Tap tap;
    private List<DataStore> dataStores;

    public RawTap(Pipe pipe, Tap tap) {
      this(pipe, tap, Lists.<DataStore>newArrayList());
    }

    public RawTap(Pipe pipe, Tap tap, List<DataStore> dataStores) {
      this.pipe = pipe;
      this.tap = tap;
      this.dataStores = dataStores;
    }

    @Override
    public Pipe getPipe() {
      return pipe;
    }

    @Override
    public TapFactory getTapFactory() {
      return new TapFactory() {
        @Override
        public Tap createTap() throws IOException {
          return tap;
        }
      };
    }

    @Override
    public List<DataStore> getOutputStores() {
      return dataStores;
    }
  }

  public class PartitionedSink implements SinkBinding {

    private final Pipe pipe;
    private final PartitionableDataStore store;
    private final PartitionFactory structure;

    public PartitionedSink(Pipe pipe, PartitionableDataStore store, PartitionFactory structure) {
      this.pipe = pipe;
      this.store = store;
      this.structure = structure;
    }

    @Override
    public Pipe getPipe() {
      return pipe;
    }

    @Override
    public TapFactory getTapFactory() {
      return new TapFactory() {
        @Override
        public Tap createTap() throws IOException {
          return store.getPartitionedSinkTap(structure.create());
        }
      };
    }

    @Override
    public List<DataStore> getOutputStores() {
      return Lists.<DataStore>newArrayList(store);
    }
  }

  public class EmptySink implements SinkBinding {

    private final Pipe pipe;

    public EmptySink(Pipe pipe) {
      this.pipe = pipe;
    }

    @Override
    public Pipe getPipe() {
      return pipe;
    }

    @Override
    public TapFactory getTapFactory() {
      return new NullTapFactory();
    }

    @Override
    public List<DataStore> getOutputStores() {
      return Lists.newArrayList();
    }
  }

}
