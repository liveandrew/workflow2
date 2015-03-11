package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.PartitionedDataStore;
import com.rapleaf.cascading_ext.msj_tap.InsertEmptySplit;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.operation.MSJFunction;
import com.rapleaf.cascading_ext.msj_tap.scheme.MSJScheme;
import com.rapleaf.cascading_ext.msj_tap.store.PartionableDataStore;
import com.rapleaf.cascading_ext.msj_tap.tap.MSJTap;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.CascadingAction2;
import com.rapleaf.cascading_ext.workflow2.TapFactory;

public class MSJTapAction<K extends Comparable> extends CascadingAction2 {

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      PartionableDataStore output,
                      PartitionStructure outputStructure) {
    this(checkpointToken, tmpRoot, Maps.newHashMap(),
        inputs, function, output, outputStructure);
  }

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      Map<Object, Object> properties,
                      final ExtractorsList<K> inputs,
                      MSJFunction<K> function,
                      PartionableDataStore output,
                      PartitionStructure outputStructure) {
    super(checkpointToken, tmpRoot, properties);
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
    });

    pipe = new Each(pipe,
        function
    );

    pipe = new InsertEmptySplit(pipe);

    completePartitioned("msj-tap", pipe, output, outputStructure);
  }

  private List<InputConf<K>> getConfs(List<StoreExtractor<K>> inputs) throws IOException {
    List<InputConf<K>> conf = Lists.newArrayList();
    for (StoreExtractor input : inputs) {
      conf.add(input.getConf());
    }
    return conf;
  }

}
