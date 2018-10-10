package com.rapleaf.cascading_ext.workflow2;

import java.util.Collection;

import cascading.pipe.Pipe;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.pipe.PipeFactory;
import com.rapleaf.cascading_ext.tap.TapFactory;

public class SourceStoreBinding {

  private final Collection<? extends DataStore> stores;
  private final TapFactory tapFactory;
  private final PipeFactory pipeFactory;

  public SourceStoreBinding(Collection<? extends DataStore> stores, TapFactory tapFactory, PipeFactory pipeFactory) {
    this.stores = stores;
    this.tapFactory = tapFactory;
    this.pipeFactory = pipeFactory;
  }

  Collection<? extends DataStore> getStores() {
    return stores;
  }

  public TapFactory getTapFactory() {
    return tapFactory;
  }

  public Pipe getPipe(Pipe pipe) {
    return pipeFactory.getPipe(pipe);
  }
}