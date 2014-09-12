package com.rapleaf.cascading_ext.workflow2.action;

import com.liveramp.commons.collections.list.ListBuilder;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;

public class ExtractorsList<K extends Comparable> extends ListBuilder<StoreExtractor<K>> {
  public ExtractorsList<K> add(MapSideJoinableDataStore store, Extractor<K> extractor){
    add(new StoreExtractor<K>(store, extractor));
    return this;
  }
}
