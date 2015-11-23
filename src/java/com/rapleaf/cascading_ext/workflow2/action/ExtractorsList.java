package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Collection;

import com.liveramp.commons.collections.list.ListBuilder;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;

public class ExtractorsList<K extends Comparable> extends ListBuilder<StoreExtractor<K>> {
  public ExtractorsList<K> add(MapSideJoinableDataStore store, Extractor<K> extractor){
    add(new StoreExtractor<K>(store, extractor));
    return this;
  }

  public ExtractorsList<K> add(DataStore store, ConfFactory<K> factory){
    add(new StoreExtractor<K>(store, factory));
    return this;
  }

  @Override
  public ExtractorsList<K> add(StoreExtractor<K> item){
    super.add(item);
    return this;
  }

  @Override
  public ExtractorsList<K> addAll(Collection<StoreExtractor<K>> item){
    super.addAll(item);
    return this;
  }

}
