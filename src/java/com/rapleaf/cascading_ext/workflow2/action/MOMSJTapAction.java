package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;

import cascading.flow.Flow;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.operation.MOMSJFunction;
import com.rapleaf.cascading_ext.msj_tap.partition_mapper.IdentityPartitionMapper;
import com.rapleaf.cascading_ext.msj_tap.scheme.MSJScheme;
import com.rapleaf.cascading_ext.msj_tap.split.ApproximateLocalityMerger;
import com.rapleaf.cascading_ext.msj_tap.split.LocalityGrouper;
import com.rapleaf.cascading_ext.msj_tap.split.SplitGrouper;
import com.rapleaf.cascading_ext.msj_tap.tap.MSJTap;
import com.rapleaf.cascading_ext.tap.FieldRemap;
import com.rapleaf.cascading_ext.tap.RoutingSinkTap;
import com.rapleaf.cascading_ext.tap.bucket2.BucketTap2;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.Action;

public class MOMSJTapAction<E extends Enum<E>, Key extends Comparable> extends Action {

  private final MOMSJFunction<E, Key> function;
  private final Class<? extends LocalityGrouper> localityGrouper;
  private final List<StoreExtractor<Key>> extractors;

  private final Map<E, ? extends BucketDataStore> partitionedCategories;
  private final Map<E, ? extends BucketDataStore> unpartitionedCategories;
  private final PartitionStructure partitionStructure;

  public interface PostFlow {
    void callback(Flow flow);

    class NoOp implements PostFlow {
      @Override
      public void callback(Flow flow) {
        // no op
      }
    }
  }

  private final PostFlow callback;

  public MOMSJTapAction(String checkpointToken, String tmpRoot,
                        final ExtractorsList<Key> extractors,
                        MOMSJFunction<E, Key> function,
                        Map<E, ? extends BucketDataStore> partitionedCategories,
                        Map<E, ? extends BucketDataStore> unpartitionedCategories) throws IOException {
    this(checkpointToken, tmpRoot, extractors, function, partitionedCategories, unpartitionedCategories, new PostFlow.NoOp(), Maps.newHashMap());
  }

  public MOMSJTapAction(String checkpointToken, String tmpRoot,
                        final ExtractorsList<Key> extractors,
                        MOMSJFunction<E, Key> function,
                        Map<E, ? extends BucketDataStore> outputCategories) throws IOException {
    this(checkpointToken, tmpRoot, extractors, function, outputCategories, Maps.<E, BucketDataStore>newHashMap(), new PostFlow.NoOp(), Maps.newHashMap());
  }

  public MOMSJTapAction(String checkpointToken, String tmpRoot, Map<Object, Object> properties,
                        final ExtractorsList<Key> extractors,
                        MOMSJFunction<E, Key> function,
                        Map<E, ? extends BucketDataStore> outputCategories) throws IOException {
    this(checkpointToken, tmpRoot, extractors, function, outputCategories, Maps.<E, BucketDataStore>newHashMap(), new PostFlow.NoOp(), properties);
  }

  public MOMSJTapAction(String checkpointToken, String tmpRoot,
                        final ExtractorsList<Key> extractors,
                        MOMSJFunction<E, Key> function,
                        Map<E, ? extends BucketDataStore> partitionedCategories,
                        Map<E, ? extends BucketDataStore> unpartitionedCategories,
                        PostFlow callback,
                        Map<Object, Object> properties) throws IOException {
    this(checkpointToken, tmpRoot, extractors, function, partitionedCategories, unpartitionedCategories, callback, properties, PartitionStructure.UNENFORCED);
  }

  public MOMSJTapAction(String checkpointToken, String tmpRoot,
                        final ExtractorsList<Key> extractors,
                        MOMSJFunction<E, Key> function,
                        Map<E, ? extends BucketDataStore> partitionedCategories,
                        Map<E, ? extends BucketDataStore> unpartitionedCategories,
                        PostFlow callback,
                        Map<Object, Object> properties,
                        PartitionStructure partitionStructure) throws IOException {
    this(checkpointToken, tmpRoot, extractors,
        function, partitionedCategories, unpartitionedCategories, callback, properties, SplitGrouper.class, partitionStructure);
  }

  public MOMSJTapAction(String checkpointToken, String tmpRoot,
                        final ExtractorsList<Key> extractors,
                        MOMSJFunction<E, Key> function,
                        Map<E, ? extends BucketDataStore> partitionedCategories,
                        Map<E, ? extends BucketDataStore> unpartitionedCategories,
                        PostFlow callback,
                        Map<Object, Object> properties,
                        Class<? extends LocalityGrouper> grouper,
                        PartitionStructure partitionStructure) throws IOException {
    super(checkpointToken, tmpRoot, properties);

    if (!CollectionUtils.intersection(partitionedCategories.keySet(), unpartitionedCategories.keySet()).isEmpty()) {
      throw new RuntimeException("Partitioned and unpartitioned outputs cannot overlap!");
    }

    this.function = function;
    this.localityGrouper = grouper;
    this.extractors = extractors.get();
    this.callback = callback;
    this.partitionStructure = partitionStructure;

    for (StoreExtractor input : this.extractors) {
      Collection<DataStore> dataStores = input.getStores();
      for (DataStore dataStore : dataStores) {
        readsFrom(dataStore);
      }
    }


    this.partitionedCategories = partitionedCategories;
    for (Map.Entry<E, ? extends BucketDataStore> entry : partitionedCategories.entrySet()) {
      creates(entry.getValue());
    }

    this.unpartitionedCategories = unpartitionedCategories;
    for (Map.Entry<E, ? extends BucketDataStore> entry : unpartitionedCategories.entrySet()) {
      creates(entry.getValue());
    }

  }

  @Override
  protected void execute() throws Exception {
    init(function);

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, function);

    MSJTap<Key> source = new MSJTap<Key>(getConfs(extractors), new MSJScheme<Key>(), new ApproximateLocalityMerger(localityGrouper), new IdentityPartitionMapper());

    Map<Object, BucketTap2> taps = Maps.newHashMap();
    Map<Object, String> tapToSinkFieldName = Maps.newHashMap();

    for (Map.Entry<E, ? extends BucketDataStore> entry : partitionedCategories.entrySet()) {
      populate(taps, tapToSinkFieldName, entry, entry.getValue().getPartitionedSinkTap(partitionStructure));
    }

    for (Map.Entry<E, ? extends BucketDataStore> entry : unpartitionedCategories.entrySet()) {
      populate(taps, tapToSinkFieldName, entry, entry.getValue().getTap());
    }

    Flow flow = completeWithProgress(buildFlow().connect(
        source,
        new RoutingSinkTap(
            MOMSJFunction.CATEGORY_FIELD,
            taps,
            new FieldRemap.FieldRouteRemap(MOMSJFunction.RECORD_FIELD, tapToSinkFieldName)
        ),
        pipe
    ));

    callback.callback(flow);

  }

  protected void init(MOMSJFunction<E, Key> function) throws Exception {
    // Do nothing by default.
  }

  private void populate(Map<Object, BucketTap2> taps, Map<Object, String> tapToSinkFieldName, Map.Entry<E, ? extends BucketDataStore> entry, BucketTap2 tap) {
    String fieldName = tap.getScheme().getStorageStrategy().getFieldName();
    String categoryName = entry.getKey().toString();

    taps.put(categoryName, tap);
    tapToSinkFieldName.put(categoryName, fieldName);
  }


  private List<InputConf<Key>> getConfs(List<StoreExtractor<Key>> inputs) throws IOException {
    List<InputConf<Key>> conf = Lists.newArrayList();
    for (StoreExtractor input : inputs) {
      conf.add(input.getConf());
    }
    return conf;
  }

}
