package com.rapleaf.cascading_ext.workflow2.action;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.FlowCompletedCallback;

import java.util.*;

public abstract class CascadingAction extends Action {

  private final Map<String, Tap> sources = new HashMap<String, Tap>();
  private final Map<String, Tap> sinks = new HashMap<String, Tap>();
  private Map<Object, Object> flowProperties = new HashMap<Object, Object>();
  private List<Pipe> tails = new ArrayList<Pipe>();
  private String name = null;
  private Flow flow;
  private FlowCompletedCallback flowCompletedCallback = null;

  public CascadingAction(String checkpointToken) {
    this(checkpointToken, Collections.<DataStore>emptyList(), Collections.<DataStore>emptyList());
  }

  public CascadingAction(String checkpointToken,
                         List<? extends DataStore> inputStores,
                         List<? extends DataStore> outputStores) {
    super(checkpointToken);

    for (DataStore ds : inputStores) {
      readsFrom(ds);
    }

    for (DataStore ds : outputStores) {
      creates(ds);
    }

    setUp();
  }

  protected void completeFromTail(Pipe tail) {
    completeFromTails(tail);
  }

  protected void completeFromTails(Pipe... tails) {
    Collections.addAll(this.tails, tails);
    if (sources.isEmpty() || sinks.isEmpty()) {
      throw new RuntimeException("You must specify sources and sinks. Call completeFromTails last.");
    }
    planFlow();
  }

  protected void addSourceTap(Tap source) {
    addSourceTap("singleton-source", source);
  }

  public void setName(String name) {
    this.name = name;
  }

  protected void addSourceTaps(Map<String, Tap> sources) {
    for (Map.Entry<String, Tap> source : sources.entrySet()) {
      addSourceTap(source.getKey(), source.getValue());
    }
  }

  protected void addSourceTap(String name, Tap tap) {
    if (this.sources.containsKey(name)) {
      throw new RuntimeException("sources already contains name " + name + "!");
    }

    this.sources.put(name, tap);
  }

  protected void setFlowCompletedCallback(FlowCompletedCallback callback) {
    this.flowCompletedCallback = callback;
  }

  protected void addSinkTap(Tap sink) {
    addSinkTap("singleton-sink", sink);
  }

  protected void addSinkTaps(Map<String, Tap> sinks) {
    for (Map.Entry<String, Tap> sink : sinks.entrySet()) {
      addSinkTap(sink.getKey(), sink.getValue());
    }
  }

  protected void addSinkTap(String name, Tap sink) {
    if (this.sinks.containsKey(name)) {
      throw new RuntimeException("sinks already contains name " + name + "!");
    }

    this.sinks.put(name, sink);
  }

  protected void addFlowProperties(Map<Object, Object> properties) {
    flowProperties.putAll(properties);
  }

  // override in anonymous classes
  protected void setUp() {
  }

  protected Flow planFlow() {
    if (flow == null) {
      if (name == null) {
        name = getClass().getSimpleName();
      }

      FlowConnector connector = CascadingHelper.get().getFlowConnector(flowProperties);
      if (sources.size() == 1) {
        Tap source = sources.values().iterator().next();
        if (sinks.size() == 1) {
          Tap sink = sinks.values().iterator().next();
          flow = connector.connect(name, source, sink, tails.get(0));
        } else {
          flow = connector.connect(name, source, sinks, tails);
        }
      } else {
        if (sinks.size() == 1) {
          Tap sink = sinks.values().iterator().next();
          flow = connector.connect(name, sources, sink, tails.get(0));
        } else {
          flow = connector.connect(name, sources, sinks, tails.toArray(new Pipe[tails.size()]));
        }
      }
    }
    return flow;
  }

  public Flow getFlow() {
    return flow;
  }

  @Override
  protected void execute() throws Exception {
    Flow flow = getFlow();
    completeWithProgress(flow);
    if (flowCompletedCallback != null) {
      flowCompletedCallback.flowCompleted(flow);
    }
  }
}
