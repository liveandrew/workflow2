package com.rapleaf.cascading_ext.workflow2.action;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.operation.MSJFunction;
import com.rapleaf.cascading_ext.msj_tap.scheme.MSJScheme;
import com.rapleaf.cascading_ext.msj_tap.store.MapSideJoinableDataStore;
import com.rapleaf.cascading_ext.msj_tap.tap.MSJTap;
import com.rapleaf.cascading_ext.workflow2.CascadingAction2;
import com.rapleaf.cascading_ext.workflow2.TapFactory;

public class MSJTapAction<K extends Comparable> extends CascadingAction2 {

  public MSJTapAction(String checkpointToken, String tmpRoot,
                      Map<Object, Object> properties,
                      final List<StoreExtractor<K>> inputs,
                      MSJFunction<K> function,
                      BucketDataStore output) {
    super(checkpointToken, tmpRoot, properties);

    List<DataStore> dsStores = Lists.newArrayList();
    for (StoreExtractor input : inputs) {
      MapSideJoinableDataStore store = input.getStore();
      if(store instanceof DataStore){
        dsStores.add((DataStore) store);
      }
    }

    Pipe pipe = bindSource("msj-tap", dsStores, new TapFactory() {
      @Override
      public Tap createTap() throws IOException {
        return new MSJTap<K>(getConfs(inputs), new MSJScheme<K>());
      }
    });

    pipe = new Each(pipe,
        function
    );

    completePartitioned("msj-tap", pipe, output);

  }

  private List<InputConf<K>> getConfs(List<StoreExtractor<K>> inputs) throws IOException {
    List<InputConf<K>> conf = Lists.newArrayList();
    for (StoreExtractor input : inputs) {
      conf.add(input.getStore().getInputConf(input.getExtractor()));
    }
    return conf;
  }

}
