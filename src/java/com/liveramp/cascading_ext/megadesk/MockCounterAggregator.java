package com.liveramp.cascading_ext.megadesk;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.recipes.aggregator.InterProcessKeyedAggregator;

public class MockCounterAggregator<GROUP, NAME> extends BaseCounterAggregator<GROUP, NAME> {

  private static InterProcessKeyedAggregator aggregator
      = new InterProcessKeyedAggregator(
      new InMemoryLocal(),
      new CountingAggregator()
  );


  @Override
  protected InterProcessKeyedAggregator<CounterId<GROUP, NAME>, Long, Long> getAggregator() {
    return aggregator;
  }
}
