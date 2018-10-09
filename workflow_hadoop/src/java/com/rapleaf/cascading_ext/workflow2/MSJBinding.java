package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.map_side_join.IExtractor;

public class MSJBinding<T extends Comparable> {

  private final IExtractor<T> extractor;

  public MSJBinding(IExtractor<T> extractor) {
    this.extractor = extractor;
  }

  public IExtractor<T> getExtractor() {
    return extractor;
  }
}
