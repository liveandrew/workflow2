package com.rapleaf.cascading_ext.workflow2.flow_closure;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.cascading_ext.CascadingHelper;

public class CascadingHelperRunner implements FlowRunner {
  @Override
  public Flow complete(Properties properties, String name, Tap source, Tap sink, Pipe tail) {
    Flow flow = CascadingHelper.get().getFlowConnector(properties).connect(name, source, sink, tail);
    flow.complete();
    return flow;
  }
}
