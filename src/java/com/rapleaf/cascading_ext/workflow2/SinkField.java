package com.rapleaf.cascading_ext.workflow2;

import cascading.pipe.Pipe;
import com.rapleaf.cascading_ext.map_side_join.Extractor;

public class SinkField {
  private final String fieldName;
  private final Class fieldClass;
  private final Extractor extractor;
  private final Pipe pipe;

  public SinkField(String fieldName, Class fieldClass, Extractor extractor, Pipe pipe) {
    this.fieldName = fieldName;
    this.fieldClass = fieldClass;
    this.extractor = extractor;
    this.pipe = pipe;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Class getFieldClass() {
    return fieldClass;
  }

  public Pipe getPipe() {
    return pipe;
  }

  public Extractor getExtractor() {
    return extractor;
  }
}
