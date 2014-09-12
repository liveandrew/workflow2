package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;

public class StoreExtractor<K extends Comparable> {
  private final MapSideJoinableDataStore store;
  private final Extractor<K> extractor;

  public StoreExtractor(MapSideJoinableDataStore store, Extractor<K> extractor) {
    this.store = store;
    this.extractor = extractor;
  }

  public MapSideJoinableDataStore getStore() {
    return store;
  }

  public Extractor<K> getExtractor() {
    return extractor;
  }
}
