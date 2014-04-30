package com.liveramp.cascading_ext.megadesk;

import com.google.common.collect.ImmutableMap;

import com.liveramp.megadesk.base.state.Local;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.recipes.aggregator.InterProcessKeyedAggregator;
import com.rapleaf.cascading_ext.queues.LiverampQueues;

public class ProductionCounterAggregator<GROUP, NAME> extends BaseCounterAggregator<GROUP, NAME> {

  private final String name;
  private transient InterProcessKeyedAggregator<CounterId<GROUP, NAME>, Long, Long> aggregator;

  public ProductionCounterAggregator(String uniqueName) {
    this.name = uniqueName;
  }

  @Override
  protected InterProcessKeyedAggregator<BaseCounterAggregator.CounterId<GROUP, NAME>, Long, Long> getAggregator() {
    if (aggregator == null) {
      Driver driver = getMegadeskSource().makeDriverFactory("/clockwork/counting-aggregator").get(name, ImmutableMap.of());
      aggregator = new InterProcessKeyedAggregator<BaseCounterAggregator.CounterId<GROUP, NAME>, Long, Long>(new Local(driver), new CountingAggregator());
    }
    return aggregator;
  }

  protected LiverampQueues getMegadeskSource() {
    return LiverampQueues.getProduction();
  }
}
