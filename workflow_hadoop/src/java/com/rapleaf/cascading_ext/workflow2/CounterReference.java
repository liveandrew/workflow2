package com.rapleaf.cascading_ext.workflow2;

import java.util.concurrent.Callable;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;

public class CounterReference {

  private Callable<TwoNestedMap<String, String, Long>> ref;

  public void set(Callable<TwoNestedMap<String, String, Long>> reference) {
    this.ref = reference;
  }

  public Callable<TwoNestedMap<String, String, Long>> get() {
    return ref;
  }
}
