package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

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
  private final Map<E, ? extends BucketDataStore> outputCategories;
  private final BucketDataStore<BytesWritable> tmpPartitioned;

  public MOMSJTapAction(String checkpointToken, String tmpRoot,
                        final ExtractorsList<BytesWritable> extractors,
                        MOMSJFunction<E, BytesWritable> function,
                        Map<E, ? extends BucketDataStore> outputCategories) throws IOException {
    super(checkpointToken, tmpRoot);

    this.function = function;
    this.extractors = extractors.get();
    this.outputCategories = outputCategories;

    tmpPartitioned = builder().getBucketDataStore("tmp_partitioned", BytesWritable.class);

    for (StoreExtractor input : this.extractors) {
      readsFrom(input.getStore());
    }

    for (BucketDataStore store : outputCategories.values()) {
      creates(store);
    }

  }

  @Override
  protected void execute() throws Exception {

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, function);

    MSJTap<BytesWritable> source = new MSJTap<BytesWritable>(getConfs(extractors), new MSJScheme<BytesWritable>());

    completeWithProgress(buildFlow().connect(
        source,
        new PartitionedBucketTap<BytesWritable>(tmpPartitioned.getPath(),
            new BytesStorageStrategy(MOMSJFunction.RECORD_FIELD),
            PartitionStructure.UNENFORCED),
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

  }

  private List<InputConf<BytesWritable>> getConfs(List<StoreExtractor<BytesWritable>> inputs) throws IOException {
    List<InputConf<BytesWritable>> conf = Lists.newArrayList();
    for (StoreExtractor input : inputs) {
      conf.add(input.getConf());
    }
    return conf;
  }

}
