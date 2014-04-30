package com.liveramp.cascading_ext.megadesk;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.liveramp.megadesk.recipes.aggregator.Aggregator;
import com.liveramp.megadesk.recipes.aggregator.InterProcessKeyedAggregator;
import com.rapleaf.support.collections.nested_map.TwoNestedMap;

public abstract class BaseCounterAggregator<GROUP, NAME> implements CounterAggregator<GROUP, NAME>, Serializable {


  protected abstract InterProcessKeyedAggregator<CounterId<GROUP, NAME>, Long, Long> getAggregator();

  @Override
  public void increment(GROUP group, NAME name, Long amount) {
    getAggregator().aggregate(new CounterId<GROUP, NAME>(group, name), amount);
  }

  @Override
  public TwoNestedMap<GROUP, NAME, Long> read() {
    try {
      ImmutableMap<CounterId<GROUP, NAME>, Long> read = getAggregator().read();
      TwoNestedMap<GROUP, NAME, Long> output = new TwoNestedMap<GROUP, NAME, Long>();
      for (Map.Entry<CounterId<GROUP, NAME>, Long> entry : read.entrySet()) {
        output.put(entry.getKey().group, entry.getKey().name, entry.getValue());
      }
      return output;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush() {
    try {
      getAggregator().flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static class CounterId<GROUP, NAME> implements Serializable {
    public final GROUP group;
    public final NAME name;

    private CounterId(GROUP group, NAME name) {
      this.name = name;
      this.group = group;
    }
  }

  static class CountingAggregator implements Aggregator<Long, Long> {

    @Override
    public Long initialValue() {
      return 0l;
    }

    @Override
    public Long aggregate(Long lhs, Long rhs) {
      return lhs + rhs;
    }

    @Override
    public Long merge(Long lhs, Long rhs) {
      return lhs + rhs;
    }
  }

  @Override
  public void intialize() {
    try {
      getAggregator().initialize();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
