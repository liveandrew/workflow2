package com.rapleaf.cascading_ext.workflow2;

import java.util.Collection;

import com.rapleaf.cascading_ext.datastore.UnitDataStore;
import com.rapleaf.cascading_ext.pipe.PipeFactory;
import com.rapleaf.cascading_ext.tap.TapFactory;

public class TypedStoreBinding<S, T extends TapFactory> extends SourceStoreBinding {

  private final Collection<? extends UnitDataStore<? extends S>> stores;
  private final T tapFactory;

  public TypedStoreBinding(Collection<? extends UnitDataStore<? extends S>> stores, T tapFactory, PipeFactory pipeFactory) {
    super(stores, tapFactory, pipeFactory);
    this.stores = stores;
    this.tapFactory = tapFactory;
  }

  @Override
  Collection<? extends UnitDataStore<? extends S>> getStores() {
    return stores;
  }

  @Override
  T getTapFactory() {
    return tapFactory;
  }
}
