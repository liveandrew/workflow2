package com.rapleaf.cascading_ext.workflow2;

import cascading.pipe.Pipe;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.TapFactory.NullTapFactory;
import com.rapleaf.cascading_ext.workflow2.TapFactory.SimpleFactory;

public interface SinkBinding {
  public Pipe getPipe();
  public TapFactory getTapFactory();

  public class DSSink implements SinkBinding{
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

  public class EmptySink implements SinkBinding {

    private final Pipe pipe;
    public EmptySink(Pipe pipe){
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
