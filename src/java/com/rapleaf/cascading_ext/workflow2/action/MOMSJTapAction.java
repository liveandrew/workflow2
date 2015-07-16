package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

import cascading.flow.Flow;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.operation.MOMSJFunction;
import com.rapleaf.cascading_ext.msj_tap.scheme.MSJScheme;
import com.rapleaf.cascading_ext.msj_tap.tap.MSJTap;
import com.rapleaf.cascading_ext.tap.bucket2.CategoryBucketTap;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionedBucketTap;
import com.rapleaf.cascading_ext.tap.bucket2.storage.BytesStorageStrategy;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.formats.bucket.Bucket;

public class MOMSJTapAction<E extends Enum<E>> extends Action {

  private final MOMSJFunction<E, BytesWritable> function;
  private final List<StoreExtractor<BytesWritable>> extractors;
  private final Map<E, BucketDataStore> outputCategories;
  private final BucketDataStore<BytesWritable> tmpPartitioned;
  private final Set<String> unpartitionedOutputs;

  public interface PostFlow {
    public void callback(Flow flow);

    public class NoOp implements PostFlow {
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
    super(checkpointToken, tmpRoot, properties);

    if (!CollectionUtils.intersection(partitionedCategories.keySet(), unpartitionedCategories.keySet()).isEmpty()) {
      throw new RuntimeException("Partitioned and unpartitioned outputs cannot overlap!");
    }

    this.function = function;
    this.extractors = extractors.get();
    this.callback = callback;

    tmpPartitioned = builder().getBucketDataStore("tmp_partitioned", BytesWritable.class);

    createsTemporary(tmpPartitioned);

    for (StoreExtractor input : this.extractors) {
      readsFrom(input.getStore());
    }

    this.outputCategories = Maps.newHashMap();
    this.unpartitionedOutputs = asStrings(unpartitionedCategories.keySet());

    for (Map.Entry<E, ? extends BucketDataStore> entry : partitionedCategories.entrySet()) {
      outputCategories.put(entry.getKey(), entry.getValue());
      creates(entry.getValue());
    }

    for (Map.Entry<E, ? extends BucketDataStore> entry : unpartitionedCategories.entrySet()) {
      outputCategories.put(entry.getKey(), entry.getValue());
      creates(entry.getValue());
    }

  }

  private Set<String> asStrings(Set<E> values){
    Set<String> out = Sets.newHashSet();
    for (E value : values) {
      out.add(value.toString());
    }
    return out;
  }

  @Override
  protected void execute() throws Exception {

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, function);

    MSJTap<BytesWritable> source = new MSJTap<BytesWritable>(getConfs(extractors), new MSJScheme<BytesWritable>());

    Flow flow = completeWithProgress(buildFlow().connect(
        source,
        new PartitionedBucketTap<>(tmpPartitioned.getPath(),
            new BytesStorageStrategy(MOMSJFunction.RECORD_FIELD),
            PartitionStructure.UNENFORCED,
            unpartitionedOutputs
        ),
        pipe
    ));

    Map<String, Path> asStrings = Maps.newHashMap();
    Map<String, Class> toClass = Maps.newHashMap();
    for (Map.Entry<E, ? extends BucketDataStore> entry : outputCategories.entrySet()) {
      String name = entry.getKey().name();
      asStrings.put(name, new Path(entry.getValue().getPath()));
      toClass.put(name, entry.getValue().getRecordsType());
    }

    Bucket bucket = tmpPartitioned.getBucket();
    CategoryBucketTap.createBucketsFromCategories(
        new Path(tmpPartitioned.getPath()),
        bucket.getFormat(),
        bucket.getFormatArgs(),
        asStrings, toClass
    );

    callback.callback(flow);

  }

  private List<InputConf<BytesWritable>> getConfs(List<StoreExtractor<BytesWritable>> inputs) throws IOException {
    List<InputConf<BytesWritable>> conf = Lists.newArrayList();
    for (StoreExtractor input : inputs) {
      conf.add(input.getConf());
    }
    return conf;
  }

}
