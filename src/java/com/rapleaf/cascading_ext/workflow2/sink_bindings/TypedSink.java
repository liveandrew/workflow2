package com.rapleaf.cascading_ext.workflow2.sink_bindings;

import org.apache.commons.lang3.tuple.MutablePair;

import com.rapleaf.cascading_ext.datastore.UnitDataStore;

public class TypedSink<T> extends MutablePair<UnitDataStore<T>, Class<T>> {
  public TypedSink(UnitDataStore<T> store, Class<T> type) {
    super(store, type);
  }

  public static <T> TypedSink<T> of(UnitDataStore<T> store, Class<T> type) {
    return new TypedSink<>(store, type);
  }

}
