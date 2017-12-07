package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.map_side_join.IExtractor;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;

public class StoreExtractor<K extends Comparable> {
  private final DataStore store;
  private final ConfFactory<K> confFactory;

  public StoreExtractor(MapSideJoinableDataStore store, IExtractor<K> confFactory) {
    this.store = store;
    this.confFactory = new ConfFactory.ExtractorConfFactory<K>(store, confFactory);
  }

  public StoreExtractor(DataStore store, ConfFactory<K> factory) {
    this.store = store;
    this.confFactory = factory;
  }

  public DataStore getStore() {
    return store;
  }

  public InputConf<K> getConf() {
    return confFactory.getInputConf();
  }

}
