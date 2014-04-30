package com.liveramp.cascading_ext.megadesk;

import com.rapleaf.support.collections.nested_map.TwoNestedMap;

public interface CounterAggregator<GROUP, NAME> {

  public void intialize();

  public void increment(GROUP group, NAME name, Long amount);

  public void flush();

  public TwoNestedMap<GROUP, NAME, Long> read();

}
