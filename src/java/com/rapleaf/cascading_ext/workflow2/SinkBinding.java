package com.rapleaf.cascading_ext.workflow2;

import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.TapFactory.NullTapFactory;
import com.rapleaf.cascading_ext.workflow2.TapFactory.SimpleFactory;

import java.io.IOException;

public interface SinkBinding {
  public Pipe getPipe();

  public TapFactory getTapFactory();

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
