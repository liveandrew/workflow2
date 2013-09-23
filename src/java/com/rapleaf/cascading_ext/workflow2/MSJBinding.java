package com.rapleaf.cascading_ext.workflow2;

import com.rapleaf.cascading_ext.map_side_join.Extractor;

public class MSJBinding<T extends Comparable> {

  private final Extractor<T> extractor;

  public MSJBinding(Extractor<T> extractor) {
    this.extractor = extractor;
  }

  public Extractor<T> getExtractor() {
    return extractor;
  }
}
