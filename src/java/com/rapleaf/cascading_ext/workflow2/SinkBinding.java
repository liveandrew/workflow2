package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;

import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.msj_tap.store.PartionableDataStore;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.TapFactory.NullTapFactory;
import com.rapleaf.cascading_ext.workflow2.TapFactory.SimpleFactory;

public interface SinkBinding {
  public Pipe getPipe();

  public TapFactory getTapFactory();

  interface DataStoreSink {
    public DataStore getOutputStore();
  }

  public class DSSink implements SinkBinding, DataStoreSink {
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

    public DataStore getOutputStore() {
      return outputStore;
    }
  }

  public class RawTap implements SinkBinding {

    private final Pipe pipe;
    private final Tap tap;

    public RawTap(Pipe pipe, Tap tap) {
      this.pipe = pipe;
      this.tap = tap;
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
  }

  public class PartitionedSink implements SinkBinding, DataStoreSink {

    private final Pipe pipe;
    private final PartionableDataStore store;
    private final PartitionStructure structure;

    public PartitionedSink(Pipe pipe, PartionableDataStore store, PartitionStructure structure){
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
          return store.getPartitionedSinkTap(structure);
        }
      };
    }

    @Override
    public DataStore getOutputStore() {
      return store;
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
  }

}
