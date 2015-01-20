package com.rapleaf.cascading_ext.workflow2.action;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;

import cascading.flow.FlowListener;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.FlowBuilder;
import com.rapleaf.cascading_ext.workflow2.TapFactory;

public class FutureCascadingAction extends Action {

  private final String flowName;
  private final Map<String, TapFactory> sourceTaps;
  private final Map<String, TapFactory> sinkTaps;
  private final List<Pipe> tails;
  private final FlowListener listener;

  public FutureCascadingAction(String checkpointToken,
                               String flowName,
                               Map<String, TapFactory> sourceTapFactories,
                               Map<String, TapFactory> sinkTapFactories,
                               List<Pipe> tails,
                               Collection<DataStore> readStores,
                               Collection<DataStore> createStores,
                               Map<Object, Object> properties,
                               FlowListener listener) {
    super(checkpointToken, properties);

    this.flowName = flowName;
    this.sourceTaps = sourceTapFactories;
    this.sinkTaps = sinkTapFactories;
    this.tails = tails;
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

    FlowBuilder.FlowClosure f = buildFlow().connect(
        flowName + ": " + getActionId().getRelativeName(),
        sourceTaps,
        sinkTaps,
        tails.toArray(new Pipe[tails.size()]));

    if(listener != null){
      f.addListener(listener);
    }

    completeWithProgress(f);
  }
}
