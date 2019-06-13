package com.liveramp.workflow2.workflow_examples.actions;

import java.util.Arrays;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.liveramp.cascading_ext.CascadingUtil;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.workflow2.Action;

public class SimpleWriteFile extends Action {

  private final TupleDataStore output;

  public SimpleWriteFile(String checkpointToken, TupleDataStore output) {
    super(checkpointToken);
    this.output = output;
    creates(output);
  }

  @Override
  protected void execute() throws Exception {

    TupleEntryCollector outputCollector = output.getTap().openForWrite(CascadingUtil.get().getFlowProcess());
    for (String s : Arrays.asList("Hello", "World", "!")) {
      outputCollector.add(new Tuple(s));
    }
    outputCollector.close();

  }
}
