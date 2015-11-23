package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.operation.MSJFunction;
import com.rapleaf.cascading_ext.msj_tap.scheme.MSJScheme;
import com.rapleaf.cascading_ext.msj_tap.store.PartitionableDataStore;
import com.rapleaf.cascading_ext.msj_tap.tap.MSJTap;
import com.rapleaf.cascading_ext.tap.TapFactory;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionedBucketScheme;
import com.rapleaf.cascading_ext.tap.bucket2.ThriftBucketScheme;
import com.rapleaf.cascading_ext.workflow2.ActionCallback;
import com.rapleaf.cascading_ext.workflow2.CascadingAction2;
import com.rapleaf.cascading_ext.workflow2.PartitionFactory;

public class MSJTapAction<K extends Comparable> extends CascadingAction2 {

  private final MSJFunction<K> function;

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      PartitionableDataStore output,
                      PartitionStructure outputStructure) {
    this(checkpointToken, tmpRoot, Maps.newHashMap(),
        inputs, function, output, outputStructure);
  }

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      PartitionableDataStore output,
                      PartitionStructure outputStructure,
                      ActionCallback callback) {
    this(checkpointToken, tmpRoot, Maps.newHashMap(),
        inputs, function, output, outputStructure, callback);
  }

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      Map<Object, Object> properties,
                      final ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      PartitionableDataStore output,
                      PartitionStructure outputStructure,
                      ActionCallback callback) {
    this(checkpointToken, tmpRoot, properties, inputs, function, new SplitGenerator.Empty(), output, new PartitionFactory.Now(outputStructure), callback);
  }

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      Map<Object, Object> properties,
                      final ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      PartitionableDataStore output,
                      PartitionStructure outputStructure) {
    this(checkpointToken, tmpRoot, properties, inputs, function, new SplitGenerator.Empty(), output, outputStructure);
  }


  public MSJTapAction(String checkpointToken, String tmpRoot,
                      final ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      SplitGenerator splitGen,
                      PartitionableDataStore output,
                      PartitionStructure outputStructure) {
    this(checkpointToken, tmpRoot, Maps.newHashMap(), inputs, function, splitGen, output, new PartitionFactory.Now(outputStructure));
  }

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      Map<Object, Object> properties,
                      final ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      SplitGenerator splitGen,
                      PartitionableDataStore output,
                      PartitionStructure outputStructure) {
    this(checkpointToken, tmpRoot, properties, inputs, function, splitGen, output, new PartitionFactory.Now(outputStructure));
  }

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      Map<Object, Object> properties,
                      final ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      SplitGenerator splitGen,
                      PartitionableDataStore output,
                      PartitionFactory structureFactory) {
    this(checkpointToken, tmpRoot,
        properties,
        inputs,
        function,
        splitGen,
        output,
        structureFactory,
        new ActionCallback.Default());
  }

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      Map<Object, Object> properties,
                      final ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      SplitGenerator splitGen,
                      PartitionableDataStore output,
                      PartitionFactory structureFactory,
                      ActionCallback callback) {
    super(checkpointToken, tmpRoot, properties);

    this.function = function;

    final List<StoreExtractor<K>> asList = inputs.get();

    List<DataStore> dsStores = Lists.newArrayList();
    for (StoreExtractor input : asList) {
      DataStore store = input.getStore();
      dsStores.add(store);
    }

    Pipe pipe = bindSource("msj-tap", dsStores, new TapFactory() {
      @Override
      public Tap createTap() throws IOException {
        return new MSJTap<K>(getConfs(asList), new MSJScheme<K>());
      }
    }, callback);

    pipe = new Each(pipe,
        function
    );

    pipe = new Each(pipe,
        new Fields(ThriftBucketScheme.getFieldName(output.getRecordsType())),
        new InsertSplit(splitGen),
        Fields.ALL);

    completePartitioned("msj-tap", pipe, output, structureFactory);
  }



  //  TODO this is ugly and I'd rather figure out a way to let the user pass in a MSJFunctionFactory... but it's tough.
  protected MSJFunction<K> getFunction() {
    return function;
  }

  private static class InsertSplit extends BaseOperation implements Function {

    private final SplitGenerator generator;

    public InsertSplit(SplitGenerator generator) {
      super(new Fields(PartitionedBucketScheme.SPLIT_FIELD));
      this.generator = generator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      functionCall.getOutputCollector().add(new Tuple(generator.getSplit(functionCall.getArguments().getObject(0))));
    }
  }

  private List<InputConf<K>> getConfs(List<StoreExtractor<K>> inputs) throws IOException {
    List<InputConf<K>> conf = Lists.newArrayList();
    for (StoreExtractor input : inputs) {
      conf.add(input.getConf());
    }
    return conf;
  }

}
