package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Arrays;
import java.util.List;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.map_side_join.MapSideJoin;
import com.rapleaf.cascading_ext.map_side_join.joins.ChooseNewest;
import com.rapleaf.cascading_ext.workflow2.Action;

public class ChooseNewestAction<T extends Comparable> extends Action {
  private final List<Extractor<T>> extractors;
  private final BucketDataStore oldStore;
  private final BucketDataStore newStore;
  private final BucketDataStore results;

  public ChooseNewestAction(
      String checkpointToken,
      List<Extractor<T>> extractors,
      BucketDataStore oldStore,
      BucketDataStore newStore,
      BucketDataStore results) {
    super(checkpointToken);
    this.extractors = extractors;
    this.oldStore = oldStore;
    this.newStore = newStore;
    this.results = results;

    readsFrom(oldStore);
    readsFrom(newStore);
    creates(results);
  }

  @Override
  protected void execute() throws Exception {
    MapSideJoin<T> join = new MapSideJoin<T>(extractors, new ChooseNewest<T>(), Arrays.asList(oldStore, newStore), results);
    join.run();
  }
}
