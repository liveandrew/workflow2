package com.rapleaf.cascading_ext.workflow2.action;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.google.common.collect.Maps;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.TapFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class FutureCascadingAction extends Action {

  private final Map<String, TapFactory> sourceTaps;
  private final Map<String, TapFactory> sinkTaps;
  private final List<Pipe> tails;
  private final Map<Object, Object> properties;
  private final FlowListener listener;

  public FutureCascadingAction(String checkpointToken,
                               Map<String, TapFactory> sourceTapFactories,
                               Map<String, TapFactory> sinkTapFactories,
                               List<Pipe> tails,
                               Collection<DataStore> readStores,
                               Collection<DataStore> createStores,
                               Map<Object, Object> properties,
                               FlowListener listener) {
    super(checkpointToken);

    this.sourceTaps = sourceTapFactories;
    this.sinkTaps = sinkTapFactories;
    this.tails = tails;
    this.properties = properties;
    this.listener = listener;

    for (DataStore readStore : readStores) {
      readsFrom(readStore);
    }

    for (DataStore createStore : createStores) {
      creates(createStore);
    }
  }

  @Override
  protected void execute() throws Exception {

    Map<String, Tap> sourceTaps = Maps.newHashMap();
    for (Entry<String, TapFactory> entry : this.sourceTaps.entrySet()) {
      sourceTaps.put(entry.getKey(), entry.getValue().createTap());
    }

    Map<String, Tap> sinkTaps = Maps.newHashMap();
    for (Entry<String, TapFactory> entry : this.sinkTaps.entrySet()) {
      sinkTaps.put(entry.getKey(), entry.getValue().createTap());
    }

    Flow f = CascadingHelper.get().getFlowConnector(properties).connect(
        sourceTaps,
        sinkTaps,
        tails.toArray(new Pipe[tails.size()]));

    if(listener != null){
      f.addListener(listener);
    }

    completeWithProgress(f);
  }
}
