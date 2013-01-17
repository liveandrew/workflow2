package com.rapleaf.cascading_ext.workflow2.action;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class CascadingAction extends Action {

  private Map<String, Tap> sources;
  private Map<String, Tap> sinks;
  private Map<Object, Object> flowProperties = Collections.emptyMap();

  public CascadingAction(String checkpointToken,
                         List<DataStore> inputStores,
                         List<DataStore> outputStores) {
    super(checkpointToken);

    for(DataStore ds: inputStores){
      readsFrom(ds);
    }

    for(DataStore ds: outputStores){
      creates(ds);
    }
  }

  protected void setSource(Tap source){
    setSources(Collections.singletonMap("source", source));
  }

  protected void setSources(Map<String, Tap> sources){
    if(this.sources != null){
      throw new RuntimeException("sources already set!");
    }

    this.sources = sources;
  }

  protected void setSink(Tap sink){
    setSinks(Collections.singletonMap("sink", sink));
  }

  protected void setSinks(Map<String, Tap> sinks){
    if(this.sinks != null){
      throw new RuntimeException("source already set!");
    }

    this.sinks = sinks;
  }

  protected void addFlowProperties(Map<Object, Object> properties){
    flowProperties.putAll(properties);
  }

  @Override
  protected void execute() throws Exception {
    List<Pipe> tails = getTails();
    String name = getClass().getSimpleName();

    FlowConnector connector = CascadingHelper.get().getFlowConnector(flowProperties);
    Flow f;
    if(sources.size() == 1){
      Tap source = sources.values().iterator().next();
      if(sinks.size() == 1){
        Tap sink = sinks.values().iterator().next();
        f = connector.connect(name, source, sink, tails.get(0));
      }else{
        f = connector.connect(name, source, sinks, tails);
      }
    }else{
      if(sinks.size() == 1){
        Tap sink = sinks.values().iterator().next();
        f = connector.connect(name, sources, sink, tails.get(0));
      }else{
        f = connector.connect(name, sources, sinks, tails.toArray(new Pipe[tails.size()]));
      }
    }

    completeWithProgress(f);
  }

  protected abstract List<Pipe> getTails();
}
