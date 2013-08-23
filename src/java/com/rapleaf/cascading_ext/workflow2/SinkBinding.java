package com.rapleaf.cascading_ext.workflow2;

import cascading.pipe.Pipe;
import com.rapleaf.cascading_ext.datastore.DataStore;

public class SinkBinding {
  private final Pipe pipe;
  private final DataStore outputStore;


  public SinkBinding(Pipe pipe, DataStore outputStore) {
    this.pipe = pipe;
    this.outputStore = outputStore;
  }

  public Pipe getPipe() {
    return pipe;
  }

  public DataStore getOutputStore() {
    return outputStore;
  }
}
