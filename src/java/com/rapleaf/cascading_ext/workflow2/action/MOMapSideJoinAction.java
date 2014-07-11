package com.rapleaf.cascading_ext.workflow2.action;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.CategoryBucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.map_side_join.MOJoiner;
import com.rapleaf.cascading_ext.map_side_join.MOMapSideJoin;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.action_operations.HadoopOperation;

public abstract class MOMapSideJoinAction<T extends Comparable, E extends Enum<E>> extends Action {

  private final List<MapSideJoinableDataStore> inputStores = new ArrayList<MapSideJoinableDataStore>();
  private final CategoryBucketDataStore outputStore;
  private final Map<E, Path> categoryToPath;
  private List<Extractor<T>> extractors = new ArrayList<Extractor<T>>();
  private MOJoiner<T, E> joiner = null;
  private final Map<E, BucketDataStore> categoryToOutputBucket;
  private final Map<Object, Object> properties = Maps.newHashMap();

  public MOMapSideJoinAction(String checkpointToken,
                             String tmpRoot,
                             List<? extends MapSideJoinableDataStore> inputStores,
                             Map<E, BucketDataStore> categoryToOutputBucket) {
    super(checkpointToken, tmpRoot);
    this.categoryToPath = Maps.newHashMap();
    this.categoryToOutputBucket = categoryToOutputBucket;
    for (Map.Entry<E, BucketDataStore> entry : categoryToOutputBucket.entrySet()) {
      categoryToPath.put(entry.getKey(), new Path(entry.getValue().getPath()));
    }

    this.inputStores.addAll(inputStores);

    //We're moving to different output buckets, so the we can just make a temp store
    this.outputStore = builder().getBinaryCategoryBucketDataStore("temporary_output");
    createsTemporary(outputStore);

    for (MapSideJoinableDataStore ds : inputStores) {
      if (ds instanceof DataStore) {
        readsFrom((DataStore)ds);
      }
    }

    for (BucketDataStore outputBucket : categoryToOutputBucket.values()) {
      creates(outputBucket);
    }

    setUp();
  }

  // override in anonymous classes
  protected void setUp() {
  }

  protected void tearDown(Counters counters) {
  }

  protected void addExtractors(List<Extractor<T>> extractors) {
    this.extractors.addAll(extractors);
  }

  protected void setJoiner(MOJoiner<T, E> joiner) {
    if (this.joiner != null) {
      throw new RuntimeException("Joiner already set!");
    }
    this.joiner = joiner;
  }

  protected void addProperties(Map<Object, Object> properties){
    this.properties.putAll(properties);
  }

  @Override
  protected void execute() throws Exception {

    Map<E, Class<?>> categoryToRecordType = new HashMap<E, Class<?>>();
    for (Map.Entry<E, BucketDataStore> categoryEntry : categoryToOutputBucket.entrySet()) {
      categoryToRecordType.put(categoryEntry.getKey(), categoryEntry.getValue().getRecordsType());
    }


    MOMapSideJoin<T, E> join = new MOMapSideJoin<T, E>(this.getClass().getSimpleName(),
        extractors,
        joiner,
        inputStores,
        categoryToOutputBucket);
    join.addProperties(properties);

    completeWithProgress(new HadoopOperation(join));

    tearDown(join.getJobCounters());
  }
}
