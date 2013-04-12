package com.rapleaf.cascading_ext.workflow2;

import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.workflow2.action.CascadingAction;
import com.rapleaf.cascading_ext.workflow2.action.CascadingActionBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EasyWorkflow {

  Map<String, Tap> sources = Maps.newHashMap();
  TupleDataStore checkpointStore;
  String workingDir;
  Map<String, Tap> sinks = Maps.newHashMap();
  List<Pipe> tails = Lists.newArrayList();
  DataStoreBuilder dsBuilder;
  List<DataStore> inputs;
  List<DataStore> outputs;
  Step previousStep;
  Pipe currentPipe;

  public EasyWorkflow(String workingDir) {
    this.workingDir = workingDir;
    dsBuilder = new DataStoreBuilder(workingDir);
    inputs = Lists.newArrayList();
    outputs = Lists.newArrayList();
    sinks = Maps.newHashMap();
    sources = Maps.newHashMap();
    tails = Lists.newArrayList();
  }

  public void addSourceTap(String pipeName, Tap tap) {
    sources.put(pipeName, tap);
  }

  public void addSinkTap(String pipeName, Tap tap) {
    sinks.put(pipeName, tap);
  }

  public void addTail(Pipe tail) {
    tails.add(tail);
  }

  public void setInputs(List<DataStore> inputs) {
    this.inputs = inputs;
  }

  public void addInput(DataStore input){
    this.inputs.add(input);
  }

  public void addOutput(DataStore output){
    this.outputs.add(output);
  }

  public void setOutputs(List<DataStore> outputs) {
    this.outputs = outputs;
  }

  public Step complete(Pipe endPipe, String checkpointName) {
    CascadingActionBuilder builder = new CascadingActionBuilder();

    CascadingAction action = builder.setName(checkpointName)
        .addInputStore(checkpointStore)
        .addOutputStores(outputs)
        .addSource(currentPipe.getName(), checkpointStore.getTap())
        .setSinks(sinks)
        .addTail(endPipe)
        .build();

    Step finalStep = new Step(action, previousStep);
    return finalStep;
  }

  public Pipe addCheckpoint(Pipe endPipe, String nextPipeName, Fields fields, String checkpointName) {
    try {
      CascadingActionBuilder builder = new CascadingActionBuilder();

      if (previousStep == null) {
        checkpointStore = dsBuilder.getTupleDataStore(checkpointName, fields);

        CascadingAction action = builder.setName(checkpointName)
            .addInputStores(inputs)
            .addOutputStore(checkpointStore)
            .setSources(sources)
            .addSink(endPipe.getName(), checkpointStore.getTap())
            .addTail(endPipe)
            .build();

        Step step = new Step(action);
        previousStep = step;


      } else {

        TupleDataStore newCheckpointStore = dsBuilder.getTupleDataStore(checkpointName, fields);

        CascadingAction action = builder.setName(checkpointName)
            .addInputStore(checkpointStore)
            .addOutputStore(newCheckpointStore)
            .addSource(currentPipe.getName(), checkpointStore.getTap())
            .addSink(endPipe.getName(), newCheckpointStore.getTap())
            .addTail(endPipe)
            .build();

        Step step = new Step(action, previousStep);
        previousStep = step;
        checkpointStore = newCheckpointStore;


      }

      Pipe pipe = new Pipe(nextPipeName);
      currentPipe = pipe;
      return currentPipe;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }


  }


}
