package com.rapleaf.cascading_ext.workflow2.action;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.map_side_join.Joiner;
import com.rapleaf.cascading_ext.map_side_join.MapSideJoin;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.action_operations.HadoopOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapSideJoinAction2<T extends Comparable> extends Action {

  private final List<BucketDataStore> inputStores = new ArrayList<BucketDataStore>();
  private final BucketDataStore outputStore;
  private List<Extractor<T>> extractors;
  private final Joiner<T> joiner;
  private final Map<Object, Object> properties;

  public MapSideJoinAction2(String checkpointToken,
                            Joiner<T> joiner,
                            List<? extends BucketDataStore> inputStores,
                            List<Extractor<T>> extractors,
                            BucketDataStore outputStore,
                            Map<Object, Object> properties) {
    super(checkpointToken);

    this.inputStores.addAll(inputStores);
    this.outputStore = outputStore;
    this.extractors = extractors;
    this.properties = properties;
    this.joiner = joiner;

    for (DataStore ds : inputStores) {
      readsFrom(ds);
    }

    creates(outputStore);
  }

  protected void addExtractors(List<? extends Extractor<T>> extractors) {
    this.extractors.addAll(extractors);
  }

  @Override
  protected void execute() throws Exception {
    MapSideJoin<T> join = new MapSideJoin<T>(this.getClass().getSimpleName(),
        extractors,
        joiner,
        inputStores,
        outputStore);
    join.addProperties(this.properties);

    completeWithProgress(new HadoopOperation(join));
  }
}