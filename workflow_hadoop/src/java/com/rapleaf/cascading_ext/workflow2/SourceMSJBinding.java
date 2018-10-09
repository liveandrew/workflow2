package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.IExtractor;


public class SourceMSJBinding<T extends Comparable> extends MSJBinding<T> {
  private final BucketDataStore store;

  public SourceMSJBinding(IExtractor<T> extractor, BucketDataStore store) {
    super(extractor);
    this.store = store;
  }

  public BucketDataStore getStore() {
    return store;
  }
}
