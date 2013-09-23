package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.Extractor;

public class SourceMSJBinding<T extends Comparable> {

  private final Extractor<T> extractor;
  private final BucketDataStore store;

  public SourceMSJBinding(Extractor<T> extractor, BucketDataStore store) {
    this.extractor = extractor;
    this.store = store;
  }

  public Extractor<T> getExtractor() {
    return extractor;
  }

  public BucketDataStore getStore() {
    return store;
  }
}
