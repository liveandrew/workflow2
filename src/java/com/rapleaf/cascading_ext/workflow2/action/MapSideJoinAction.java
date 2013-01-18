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

public abstract class MapSideJoinAction<T extends Comparable> extends Action {

  private final List<BucketDataStore> inputStores = new ArrayList<BucketDataStore>();
  private final BucketDataStore outputStore;
  private List<Extractor<T>> extractors = new ArrayList<Extractor<T>>();
  private Joiner<T> joiner = null;

  public MapSideJoinAction(String checkpointToken,
                           List<? extends BucketDataStore> inputStores,
                           BucketDataStore outputStore) {
    super(checkpointToken);

    this.inputStores.addAll(inputStores);
    this.outputStore = outputStore;

    for(DataStore ds: inputStores){
      readsFrom(ds);
    }

    creates(outputStore);
  }

  protected void addExtractors(List<Extractor<T>> extractors){
    this.extractors.addAll(extractors);
  }

  protected void setJoiner(Joiner<T> joiner){
    if(this.joiner != null){
      throw new RuntimeException("Joiner already set!");
    }
    this.joiner = joiner;
  }

  @Override
  protected void execute() throws Exception {
    MapSideJoin<T> join = new MapSideJoin<T>(this.getClass().getSimpleName(),
        extractors,
        joiner,
        inputStores,
        outputStore);

    completeWithProgress(new HadoopOperation(join));
  }
}
