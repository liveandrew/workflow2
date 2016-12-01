package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Map;

import org.apache.thrift.TBase;

import cascading.pipe.Pipe;

import com.liveramp.cascading_ext.util.FieldHelper;
import com.liveramp.cascading_tools.properties.PropertiesBuilder;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.map_side_join.partitioning.PartitionAssembly;
import com.rapleaf.cascading_ext.map_side_join.partitioning.UngroupedDistributor;
import com.rapleaf.cascading_ext.tap.bucket2.FlatPartitionerFixedNumber;
import com.rapleaf.cascading_ext.tap.bucket2.partitioner.FlatPartitionerConfig;
import com.rapleaf.cascading_ext.tap.bucket2.partitioner.FlatPartitionerConfig.KeyExtractor;
import com.rapleaf.cascading_ext.workflow2.CascadingAction2;

public class ResortToPartitioned<T extends TBase> extends CascadingAction2 {
  public ResortToPartitioned(
      String checkpointToken,
      String tmpRoot,
      BucketDataStore<T> input,
      BucketDataStore<T> output,
      Class<T> klass,
      int numResortReducers,
      int numVirtualPartitions,
      KeyExtractor<T> keyAccessor
  ) {
    this(checkpointToken, tmpRoot, input, output, klass, new PropertiesBuilder().withFixedNumReduces(numResortReducers), numVirtualPartitions, keyAccessor);
  }

  public ResortToPartitioned(
      String checkpointToken,
      String tmpRoot,
      BucketDataStore<T> input,
      BucketDataStore<T> output,
      Class<T> klass,
      PropertiesBuilder propertiesBuilder,
      int numVirtualPartitions,
      KeyExtractor<T> keyAccessor
  ) {
    this(checkpointToken, tmpRoot, input, output, klass, propertiesBuilder.build(), numVirtualPartitions, keyAccessor);
  }

  public ResortToPartitioned(
      String checkpointToken,
      String tmpRoot,
      BucketDataStore<T> input,
      BucketDataStore<T> output,
      Class<T> klass,
      Map<Object, Object> flowProperties,
      int numVirtualPartitions,
      KeyExtractor<T> keyAccessor
  ) {
    super(checkpointToken, tmpRoot, flowProperties);

    String fieldName = FieldHelper.fieldNameOf(klass);
    Pipe pipe = bindSource("pipe", input);
    pipe = new PartitionAssembly<>(pipe, fieldName, new FlatPartitionerConfig<>(numVirtualPartitions, keyAccessor, klass), new UngroupedDistributor<String>());

    completePartitioned(this.getClass().getSimpleName(), pipe, output, new FlatPartitionerFixedNumber(numVirtualPartitions));
  }
}
