package com.rapleaf.cascading_ext.workflow2.action;

import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.rapleaf.cascading_ext.datastore.DataStore;

import java.util.List;
import java.util.Map;

public class CascadingActionBuilder {

  List<DataStore> inputStores = Lists.newArrayList();
  List<DataStore> outputStores = Lists.newArrayList();
  Map<String, Tap> sources = Maps.newHashMap();
  Map<String, Tap> sinks = Maps.newHashMap();
  List<Pipe> tails = Lists.newArrayList();
  String checkpoint = null;
  String name = null;


  public CascadingActionBuilder addInputStore(DataStore store) {
    inputStores.add(store);
    return this;
  }

  public CascadingActionBuilder addInputStores(List<? extends DataStore> stores) {
    inputStores.addAll(stores);
    return this;
  }

  public CascadingActionBuilder addOutputStore(DataStore store) {
    outputStores.add(store);
    return this;
  }

  public CascadingActionBuilder addOutputStores(List<? extends DataStore> stores) {
    outputStores.addAll(stores);
    return this;
  }

  public CascadingActionBuilder addSource(String name, Tap tap) {
    sources.put(name, tap);
    return this;
  }

  public CascadingActionBuilder setSources(Map<String, Tap> sources) {
    this.sources = sources;
    return this;
  }

  public CascadingActionBuilder addSink(String name, Tap tap) {
    sinks.put(name, tap);
    return this;
  }

  public CascadingActionBuilder setSinks(Map<String, Tap> sinks) {
    this.sinks = sinks;
    return this;
  }


  public CascadingActionBuilder addTail(Pipe tail) {
    tails.add(tail);
    return this;
  }

  public CascadingActionBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public CascadingActionBuilder setCheckpoint(String checkpoint) {
    this.checkpoint = checkpoint;
    return this;
  }

  public CascadingAction build() {
    return new GenericCascadingAction(checkpoint, name, inputStores, outputStores, sources, sinks, tails);
  }


  protected class GenericCascadingAction extends CascadingAction {

    public GenericCascadingAction(String checkpointToken, String name, List<? extends DataStore> inputStores, List<? extends DataStore> outputStores,
                                  Map<String, Tap> sources, Map<String, Tap> sinks, List<Pipe> tails) {
      super(checkpointToken, inputStores, outputStores);
      addSourceTaps(sources);
      addSinkTaps(sinks);
      addTails(tails.toArray(new Pipe[tails.size()]));
      setName(name);
    }
  }


}
