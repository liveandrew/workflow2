package com.rapleaf.cascading_ext.workflow2.counter;

public interface CounterFilter {
  public boolean isRecord(String group, String name);
}
