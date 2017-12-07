package com.rapleaf.cascading_ext.workflow2.action;


import java.io.IOException;

import com.rapleaf.cascading_ext.map_side_join.IExtractor;
import com.rapleaf.cascading_ext.msj_tap.conf.SeekingInputConf;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;

public class SeekingConfFactory<K extends Comparable> implements ConfFactory<K> {

  private final MapSideJoinableDataStore store;
  private final IExtractor<K> extractor;

  public SeekingConfFactory(MapSideJoinableDataStore store, IExtractor<K> extractor) {
    this.store = store;
    this.extractor = extractor;
  }

  @Override
  public SeekingInputConf<K> getInputConf() {
    try {
      return new SeekingInputConf<>(store.getInputConf(extractor));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
