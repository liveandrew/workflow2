package com.rapleaf.cascading_ext.workflow2;

import java.util.Collection;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.tap.TapFactory;

public class SourceStoreBinding {

  private final Collection<? extends DataStore> stores;
  private final TapFactory tapFactory;

  public SourceStoreBinding(Collection<? extends DataStore> stores, TapFactory tapFactory) {
    this.stores = stores;
    this.tapFactory = tapFactory;
  }

  /**
   * Intentionally package-private
   */
  Collection<? extends DataStore> getStores() {
    return stores;
  }

  /**
   * Intentionally package-private
   */
  TapFactory getTapFactory() {
    return tapFactory;
  }
}
