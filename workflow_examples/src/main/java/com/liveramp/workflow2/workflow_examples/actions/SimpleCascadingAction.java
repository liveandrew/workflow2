package com.liveramp.workflow2.workflow_examples.actions;

import java.util.Properties;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.liveramp.cascading_ext.tap.NullTap;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class SimpleCascadingAction extends Action {

  private final TupleDataStore input;

  public SimpleCascadingAction(String checkpointToken, TupleDataStore input) {
    super(checkpointToken);

    this.input = input;
    readsFrom(input);
  }

  @Override
  protected void execute() throws Exception {

    Pipe pipe = new Pipe("pipe");

    completeWithProgress(buildFlow().connect(
        input.getTap(),
        new NullTap(),
        pipe
    ));

  }
}
