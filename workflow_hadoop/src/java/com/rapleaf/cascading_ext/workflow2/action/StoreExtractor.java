package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Collection;
import java.util.Collections;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.map_side_join.IExtractor;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;

public class StoreExtractor<K extends Comparable> {
  private final Collection<DataStore> stores;
  private final ConfFactory<K> confFactory;

  public StoreExtractor(MapSideJoinableDataStore store, IExtractor<K> confFactory) {
    this.stores = Collections.<DataStore>singleton(store);
    this.confFactory = new ConfFactory.ExtractorConfFactory<K>(store, confFactory);
  }

  public StoreExtractor(DataStore store, ConfFactory<K> factory) {
    this.stores = Collections.<DataStore>singleton(store);
    this.confFactory = factory;
  }

  public StoreExtractor(Collection<DataStore> stores, ConfFactory<K> factory) {
    this.stores = stores;
    this.confFactory = factory;
  }

  public Collection<DataStore> getStores() {
    return stores;
  }

  public InputConf<K> getConf() {
    return confFactory.getInputConf();
  }

}
