package com.rapleaf.cascading_ext.workflow2;

import cascading.pipe.Pipe;
import com.rapleaf.cascading_ext.map_side_join.Extractor;

public class FlowMSJBinding<T extends Comparable> extends MSJBinding<T> {

  private final Pipe pipe;
  private final String field;
  private final Class recordType;

  public FlowMSJBinding(Extractor<T> extractor, Pipe pipe, String field, Class recordType) {
    super(extractor);
    this.pipe = pipe;
    this.field = field;
    this.recordType = recordType;
  }

  public Pipe getPipe() {
    return pipe;
  }

  public String getField() {
    return field;
  }

  public Class getRecordType() {
    return recordType;
  }
}
