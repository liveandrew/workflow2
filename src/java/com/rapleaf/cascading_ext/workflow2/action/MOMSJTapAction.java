package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.BytesWritable;

import cascading.flow.Flow;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.operation.MOMSJFunction;
import com.rapleaf.cascading_ext.msj_tap.scheme.MSJScheme;
import com.rapleaf.cascading_ext.msj_tap.tap.MSJTap;
import com.rapleaf.cascading_ext.tap.FieldRemap;
import com.rapleaf.cascading_ext.tap.RoutingSinkTap;
import com.rapleaf.cascading_ext.tap.bucket2.BucketTap2;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.Action;

public class MOMSJTapAction<E extends Enum<E>> extends Action {

  private final MOMSJFunction<E, BytesWritable> function;
  private final List<StoreExtractor<BytesWritable>> extractors;

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
                        final ExtractorsList<BytesWritable> extractors,
                        MOMSJFunction<E, BytesWritable> function,
                        Map<E, ? extends BucketDataStore> partitionedCategories,
                        Map<E, ? extends BucketDataStore> unpartitionedCategories) throws IOException {
    this(checkpointToken, tmpRoot, extractors, function, partitionedCategories, unpartitionedCategories, new PostFlow.NoOp(), Maps.newHashMap());
  }

  public MOMSJTapAction(String checkpointToken, String tmpRoot,
                        final ExtractorsList<BytesWritable> extractors,
                        MOMSJFunction<E, BytesWritable> function,
                        Map<E, ? extends BucketDataStore> outputCategories) throws IOException {
    this(checkpointToken, tmpRoot, extractors, function, outputCategories, Maps.<E, BucketDataStore>newHashMap(), new PostFlow.NoOp(), Maps.newHashMap());
  }

  public MOMSJTapAction(String checkpointToken, String tmpRoot, Map<Object, Object> properties,
                        final ExtractorsList<BytesWritable> extractors,
                        MOMSJFunction<E, BytesWritable> function,
                        Map<E, ? extends BucketDataStore> outputCategories) throws IOException {
    this(checkpointToken, tmpRoot, extractors, function, outputCategories, Maps.<E, BucketDataStore>newHashMap(), new PostFlow.NoOp(), properties);
  }

  public MOMSJTapAction(String checkpointToken, String tmpRoot,
                        final ExtractorsList<BytesWritable> extractors,
                        MOMSJFunction<E, BytesWritable> function,
                        Map<E, ? extends BucketDataStore> partitionedCategories,
                        Map<E, ? extends BucketDataStore> unpartitionedCategories,
                        PostFlow callback,
                        Map<Object, Object> properties) throws IOException {
    this(checkpointToken, tmpRoot, extractors, function, partitionedCategories, unpartitionedCategories, callback, properties, PartitionStructure.UNENFORCED);
  }

  public MOMSJTapAction(String checkpointToken, String tmpRoot,
                        final ExtractorsList<BytesWritable> extractors,
                        MOMSJFunction<E, BytesWritable> function,
                        Map<E, ? extends BucketDataStore> partitionedCategories,
                        Map<E, ? extends BucketDataStore> unpartitionedCategories,
                        PostFlow callback,
                        Map<Object, Object> properties,
                        PartitionStructure partitionStructure) throws IOException {
    super(checkpointToken, tmpRoot, properties);

    if (!CollectionUtils.intersection(partitionedCategories.keySet(), unpartitionedCategories.keySet()).isEmpty()) {
      throw new RuntimeException("Partitioned and unpartitioned outputs cannot overlap!");
    }

    this.function = function;
    this.extractors = extractors.get();
    this.callback = callback;
    this.partitionStructure = partitionStructure;

    for (StoreExtractor input : this.extractors) {
      readsFrom(input.getStore());
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

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, function);

    MSJTap<BytesWritable> source = new MSJTap<>(getConfs(extractors), new MSJScheme<BytesWritable>());

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

  private void populate(Map<Object, BucketTap2> taps, Map<Object, String> tapToSinkFieldName, Map.Entry<E, ? extends BucketDataStore> entry, BucketTap2 tap) {
    String fieldName = tap.getScheme().getStorageStrategy().getFieldName();
    String categoryName = entry.getKey().toString();

    taps.put(categoryName, tap);
    tapToSinkFieldName.put(categoryName, fieldName);
  }


  private List<InputConf<BytesWritable>> getConfs(List<StoreExtractor<BytesWritable>> inputs) throws IOException {
    List<InputConf<BytesWritable>> conf = Lists.newArrayList();
    for (StoreExtractor input : inputs) {
      conf.add(input.getConf());
    }
    return conf;
  }

}
