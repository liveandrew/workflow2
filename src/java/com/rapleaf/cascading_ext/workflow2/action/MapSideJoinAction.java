package com.rapleaf.cascading_ext.workflow2.action;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.Counters;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.map_side_join.Joiner;
import com.rapleaf.cascading_ext.map_side_join.MapSideJoin;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.action_operations.HadoopOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class MapSideJoinAction<T extends Comparable> extends Action {

  private final List<MapSideJoinableDataStore> inputStores = new ArrayList<MapSideJoinableDataStore>();
  private final BucketDataStore outputStore;
  private List<Extractor<T>> extractors = new ArrayList<Extractor<T>>();
  private Joiner<T> joiner = null;
  private ImmutableSet<String> partitionsConfinedTo = null;
  private final Map<Object, Object> properties = Maps.newHashMap();

  public MapSideJoinAction(String checkpointToken,
                           List<? extends MapSideJoinableDataStore> inputStores,
                           BucketDataStore outputStore) {
    super(checkpointToken);

    this.inputStores.addAll(inputStores);
    this.outputStore = outputStore;

    for (MapSideJoinableDataStore store : inputStores) {
      if(store instanceof DataStore){
        readsFrom((DataStore) store);
      }
    }
    creates(outputStore);

  }

  protected void addExtractors(List<? extends Extractor<T>> extractors){
    this.extractors.addAll(extractors);
  }


  protected void setJoiner(Joiner<T> joiner){
    if(this.joiner != null){
      throw new RuntimeException("Joiner already set!");
    }
    this.joiner = joiner;
  }

  protected void setPartitionsConfinedTo(Iterable<String> partitionsConfinedTo) {
    if (this.partitionsConfinedTo != null) {
      throw new RuntimeException("partitionsConfinedTo already set");
    }
    this.partitionsConfinedTo = ImmutableSet.copyOf(partitionsConfinedTo);
  }

  protected void addProperties(Map<Object, Object> properties){
    this.properties.putAll(properties);
  }

  protected void tearDown(Counters counters) {
  }

  protected String getJobName() {
    return this.getClass().getSimpleName();
  }

  @Override
  protected void execute() throws Exception {
    MapSideJoin<T> join = new MapSideJoin<T>(getJobName(),
        extractors,
        joiner,
        inputStores,
        outputStore,
        partitionsConfinedTo);
    join.addProperties(this.properties);

    completeWithProgress(new HadoopOperation(join));
    tearDown(join.getJobCounters());
  }
}
