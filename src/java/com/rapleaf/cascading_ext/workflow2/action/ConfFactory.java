package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;

import com.rapleaf.cascading_ext.map_side_join.IExtractor;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;

public interface ConfFactory<K extends Comparable> {

  InputConf<K> getInputConf();


  public static class ExtractorConfFactory<K extends Comparable> implements ConfFactory<K>{

    private final MapSideJoinableDataStore<K> store;
    private final IExtractor<K> extractor;

    public ExtractorConfFactory(MapSideJoinableDataStore<K> store, IExtractor<K> extractor) {
      this.store = store;
      this.extractor = extractor;
    }

    @Override
    public InputConf<K> getInputConf() {
      try {
        return store.getInputConf(extractor);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
