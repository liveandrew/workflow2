package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.planner.Scope;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.workflow2.action.CascadingAction;
import com.rapleaf.cascading_ext.workflow2.action.CascadingActionBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EasyWorkflow {

  private Map<String, Tap> sources = Maps.newHashMap();
  private TupleDataStore checkpointStore;
  private String workingDir;
  private Map<String, Tap> sinks = Maps.newHashMap();
  private DataStoreBuilder dsBuilder;
  private List<DataStore> inputs;
  private List<DataStore> outputs;
  private Step previousStep;
  private Pipe currentPipe;
  private String name;
  private Map<Object, Object> flowProperties;
  private int checkpoint = 0;

  private EasyWorkflow(String name, String workingDir) {
    this.workingDir = workingDir;
    this.dsBuilder = new DataStoreBuilder(workingDir);
    this.inputs = Lists.newArrayList();
    this.outputs = Lists.newArrayList();
    this.sinks = Maps.newHashMap();
    this.sources = Maps.newHashMap();
    this.name = name;
    this.flowProperties = Maps.newHashMap();
  }

  private EasyWorkflow(String name, String workingDir, Map<String, Tap> sources, Map<String, Tap> sinks) {
    this(name, workingDir);
    this.sources = sources;
    this.sinks = sinks;
  }


  public static EasyWorkflow create(String name, String workingDir, Map<String, Tap> sources, Map<String, Tap> sinks) {
    return new EasyWorkflow(name, workingDir, sources, sinks);
  }

  public static EasyWorkflow create(String name, String workingDir) {
    return new EasyWorkflow(name, workingDir);
  }

  public void setSources(Map<String, Tap> sources) {
    this.sources = sources;
  }

  public void setSinks(Map<String, Tap> sinks) {
    this.sinks = sinks;
  }

  public void addToSources(Map<String, Tap> sources) {
    this.sources.putAll(sources);
  }

  public void addToSinks(Map<String, Tap> sinks) {
    this.sinks.putAll(sinks);
  }

  public void addSourceTap(String pipeName, Tap tap) {
    sources.put(pipeName, tap);
  }

  public void addSinkTap(String pipeName, Tap tap) {
    sinks.put(pipeName, tap);
  }

  public void setInputs(List<DataStore> inputs) {
    this.inputs = inputs;
  }

  public void addInput(DataStore input) {
    this.inputs.add(input);
  }

  public void addOutput(DataStore output) {
    this.outputs.add(output);
  }

  public void setOutputs(List<DataStore> outputs) {
    this.outputs = outputs;
  }

  public Step completeAsStep(String checkpointName, Pipe... endPipes) {
    Step finalStep = createFinalStep(checkpointName, endPipes);
    return finalStep;
  }

  public MultiStepAction completeAsMultiStepAction(String checkpointName, Pipe...endPipes) {
    Step finalStep = createFinalStep(checkpointName, endPipes);
    MultiStepAction action = new GenericMultiStepAction(name, finalStep);
    return action;
  }


  public WorkflowRunner completeAsWorkflow(String checkpointName, Pipe...endPipes) {
    Step finalStep = createFinalStep(checkpointName, endPipes);
    WorkflowRunner runner = new WorkflowRunner(name, workingDir + "/checkpoints", 1, 0, finalStep);
    return runner;
  }

  private Step createFinalStep(String checkpointName, Pipe... endPipes) {
    CascadingActionBuilder builder = new CascadingActionBuilder();

    CascadingAction action = builder.setName(name + ":" + checkpointName)
        .setCheckpoint(checkpointName)
        .addInputStore(checkpointStore)
        .addOutputStores(outputs)
        .addSource(currentPipe.getName(), checkpointStore.getTap())
        .setSinks(sinks)
        .addTails(endPipes)
        .build();

    return new Step(action, previousStep);
  }

  public Pipe addCheckpoint(Pipe endPipe) {
    return addCheckpoint(endPipe, "pipe" + checkpoint, "checkpoint" + checkpoint);
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName) {
    return addCheckpoint(endPipe, "pipe" + checkpoint, checkpointName);
  }

  public Pipe addCheckpoint(Pipe endPipe, String nextPipeName, String checkpointName) {
    Fields fields;
    if (previousStep != null) {
      fields = determineOutputFields(endPipe, checkpointStore.getTap());
    } else if (sources.size() == 1) {
      fields = determineOutputFields(endPipe, sources.entrySet().iterator().next().getValue());
    } else {
      throw new IllegalArgumentException("Can't infer fields for multiple sources");
    }
    return addCheckpoint(endPipe, nextPipeName, fields, checkpointName);
  }

  private Fields determineOutputFields(Pipe tail, Tap source) {
    List<Pipe> pipes = Lists.newArrayList();
    pipes.add(tail);
    Pipe current = tail;
    while (current.getPrevious().length != 0) {
      if (current.getPrevious().length > 1) {
        throw new IllegalArgumentException("Can't infer fields for forking pipe assemblies yet");
      } else {
        pipes.add(current.getPrevious()[0]);
        current = current.getPrevious()[0];
      }
    }
    Collections.reverse(pipes);
    Set<Scope> scopes = Sets.newHashSet(new Scope(source.getSourceFields()));
    for (Pipe pipe : pipes) {
      scopes = Sets.newHashSet(pipe.outgoingScopeFor(scopes));
    }
    Scope finalScope = scopes.iterator().next();
    return finalScope.getIncomingTapFields();

  }


  public Pipe addCheckpoint(Pipe endPipe, String nextPipeName, Fields fields, String checkpointName) {
    try {

      CascadingActionBuilder builder = new CascadingActionBuilder();

      if (previousStep == null) {
        checkpointStore = dsBuilder.getTupleDataStore(checkpointName, fields);

        CascadingAction action = builder.setName(name + ":" + checkpointName)
            .setCheckpoint(checkpointName)
            .addInputStores(inputs)
            .addOutputStore(checkpointStore)
            .setSources(sources)
            .addSink(endPipe.getName(), checkpointStore.getTap())
            .addTail(endPipe)
            .addFlowProperties(flowProperties)
            .build();

        Step step = new Step(action);
        previousStep = step;


      } else {

        TupleDataStore newCheckpointStore = dsBuilder.getTupleDataStore(checkpointName, fields);

        CascadingAction action = builder.setName(name + ":" + checkpointName)
            .setCheckpoint(checkpointName)
            .addInputStore(checkpointStore)
            .addOutputStore(newCheckpointStore)
            .addSource(currentPipe.getName(), checkpointStore.getTap())
            .addSink(endPipe.getName(), newCheckpointStore.getTap())
            .addTail(endPipe)
            .addFlowProperties(flowProperties)
            .build();

        Step step = new Step(action, previousStep);
        previousStep = step;
        checkpointStore = newCheckpointStore;


      }

      Pipe pipe = new Pipe(nextPipeName);
      currentPipe = pipe;
      checkpoint++;
      return currentPipe;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }


  }

  public void addFlowProperties(Map<Object, Object> properties) {
    flowProperties.putAll(properties);
  }

  private class GenericMultiStepAction extends MultiStepAction {

    public GenericMultiStepAction(String checkpointToken, Step tail) {
      super(checkpointToken);

      setSubStepsFromTail(tail);
    }
  }

}
