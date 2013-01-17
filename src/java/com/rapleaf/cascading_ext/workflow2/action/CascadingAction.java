package com.rapleaf.cascading_ext.workflow2.action;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

import java.util.*;

public abstract class CascadingAction extends Action {

  private final Map<String, Tap> sources = new HashMap<String, Tap>();
  private final Map<String, Tap> sinks = new HashMap<String, Tap>();
  private Map<Object, Object> flowProperties = Collections.emptyMap();
  private List<Pipe> tails = new ArrayList<Pipe>();

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

  protected void addTails(List<Pipe> tails) {
    this.tails.addAll(tails);
  }

  protected void addTail(Pipe tail) {
    this.tails.add(tail);
  }

  protected void addTails(Pipe ... tails){
    Collections.addAll(this.tails, tails);
  }

  protected void addSource(Tap source){
    addSource("singleton-source", source);
  }

  protected void addSources(Map<String, Tap> sources){
    for(Map.Entry<String, Tap> source: sources.entrySet()){
      addSource(source.getKey(), source.getValue());
    }
  }

  protected void addSource(String name, Tap tap){
    if(this.sources.containsKey(name)){
      throw new RuntimeException("sources already contains name "+name+"!");
    }

    this.sources.put(name, tap);
  }
  protected void addSink(Tap sink){
    addSink("singleton-sink", sink);
  }

  protected void addSinks(Map<String, Tap> sinks){
    for(Map.Entry<String, Tap> sink: sinks.entrySet()){
      addSink(sink.getKey(), sink.getValue());
    }
  }

  protected void addSink(String name, Tap sink){
    if(this.sinks.containsKey(name)){
      throw new RuntimeException("sinks already contains name "+name+"!");
    }

    this.sinks.put(name, sink);
  }

  protected void addFlowProperties(Map<Object, Object> properties){
    flowProperties.putAll(properties);
  }

  @Override
  protected void execute() throws Exception {
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
}
