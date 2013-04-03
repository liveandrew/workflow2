package com.rapleaf.cascading_ext.workflow2.action;


import com.google.common.collect.Maps;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.CategoryBucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.map_side_join.Extractor;
import com.rapleaf.cascading_ext.map_side_join.MOJoiner;
import com.rapleaf.cascading_ext.map_side_join.MOMapSideJoin;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.action_operations.HadoopOperation;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class MOMapSideJoinAction<T extends Comparable, E extends Enum<E>> extends Action {

  private final List<BucketDataStore> inputStores = new ArrayList<BucketDataStore>();
  private final CategoryBucketDataStore outputStore;
  private final Map<E, Path> categoryToPath;
  private List<Extractor<T>> extractors = new ArrayList<Extractor<T>>();
  private MOJoiner<T, E> joiner = null;

  public MOMapSideJoinAction(String checkpointToken,
                             String tmpRoot,
                             List<? extends BucketDataStore> inputStores,
                             Map<E, BucketDataStore> categoryToOutputBucket) {
    super(checkpointToken, tmpRoot);
    this.categoryToPath = Maps.newHashMap();
    for (Map.Entry<E, BucketDataStore> entry : categoryToOutputBucket.entrySet()) {
      categoryToPath.put(entry.getKey(), new Path(entry.getValue().getPath()));
    }

    this.inputStores.addAll(inputStores);

    //We're moving to different output buckets, so the we can just make a temp store
    this.outputStore = builder().getBinaryCategoryBucketDataStore("temporary_output");
    createsTemporary(outputStore);

    for (DataStore ds : inputStores) {
      readsFrom(ds);
    }

    for (BucketDataStore outputBucket : categoryToOutputBucket.values()) {
      creates(outputBucket);
    }

    setUp();
  }

  // override in anonymous classes
  protected void setUp() {
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

  @Override
  protected void execute() throws Exception {
    MOMapSideJoin<T, E> join = new MOMapSideJoin<T, E>(this.getClass().getSimpleName(),
                                                       extractors,
                                                       joiner,
                                                       inputStores,
                                                       outputStore,
                                                       categoryToPath);

    completeWithProgress(new HadoopOperation(join));
  }
}
