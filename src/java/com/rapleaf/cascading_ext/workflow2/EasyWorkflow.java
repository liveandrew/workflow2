package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.planner.Scope;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
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
import java.util.*;

public class EasyWorkflow {

  private Map<String, Tap> sources = Maps.newHashMap();
  private TupleDataStore checkpointStore;
  private String workingDir;
  private Map<String, Tap> sinks = Maps.newHashMap();
  private List<Pipe> tails = Lists.newArrayList();
  private DataStoreBuilder dsBuilder;
  private List<DataStore> inputs;
  private List<DataStore> outputs;
  private Step previousStep;
  private Pipe currentPipe;
  private String name;

  public EasyWorkflow(String name, String workingDir) {
    this.workingDir = workingDir;
    this.dsBuilder = new DataStoreBuilder(workingDir);
    this.inputs = Lists.newArrayList();
    this.outputs = Lists.newArrayList();
    this.sinks = Maps.newHashMap();
    this.sources = Maps.newHashMap();
    this.tails = Lists.newArrayList();
    this.name = name;
  }

  public EasyWorkflow(String name, String workingDir, Map<String, Tap> sources, Map<String, Tap> sinks, Pipe... tails) {
    this.workingDir = workingDir;
    this.sources = sources;
    this.sinks = sinks;
    this.tails = Lists.newArrayList(tails);
    this.name = name;
  }

  public static EasyWorkflow create(String name, String workingDir, Map<String, Tap> sources, Map<String, Tap> sinks, Pipe... tails) {
    return new EasyWorkflow(name, workingDir, sources, sinks, tails);
  }

  public static EasyWorkflow create(String name, String workingDir, Tap source, Map<String, Tap> sinks, Pipe... tails) {

    //Copied from FlowConnector
    Set<Pipe> heads = new HashSet<Pipe>();

    for (Pipe pipe : tails) {
      Collections.addAll(heads, pipe.getHeads());
    }

    if (heads.isEmpty())
      throw new IllegalArgumentException("no pipe instance found");

    if (heads.size() != 1)
      throw new IllegalArgumentException("there may be only 1 head pipe instance, found " + heads.size());

    Map<String, Tap> sources = new HashMap<String, Tap>();

    for (Pipe pipe : heads) {
      sources.put(pipe.getName(), source);
    }

    return create(name, workingDir, sources, sinks, tails);
  }

  public static EasyWorkflow create(String name, String workingDir, Tap source, Tap sink, Pipe tail) {
    Pipe[] heads = tail.getHeads();
    if (heads.length != 1) {
      throw new IllegalArgumentException("Must be only 1 head pipe");
    }
    Map<String, Tap> sources = Collections.singletonMap(heads[0].getName(), source);
    Map<String, Tap> sinks = Collections.singletonMap(tail.getName(), sink);

    return create(name, workingDir, source, sinks, tail);
  }

  public static EasyWorkflow create(String name, String workingDir, Map<String, Tap> sources, Tap sink, Pipe tail) {
    Map<String, Tap> sinks = Collections.singletonMap(tail.getName(), sink);
    return create(name, workingDir, sources, sinks, tail);
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

  public void addInput(DataStore input) {
    this.inputs.add(input);
  }

  public void addOutput(DataStore output) {
    this.outputs.add(output);
  }

  public void setOutputs(List<DataStore> outputs) {
    this.outputs = outputs;
  }

  public Step completeAsStep(Pipe endPipe, String checkpointName) {
    Step finalStep = createFinalStep(endPipe, checkpointName);
    return finalStep;
  }

  public MultiStepAction completeAsMultiStepAction(Pipe endPipe, String checkpointName) {
    Step finalStep = createFinalStep(endPipe, checkpointName);
    MultiStepAction action = new GenericMultiStepAction(name, finalStep);
    return action;
  }


  public WorkflowRunner completeAsWorkflow(Pipe endPipe, String checkpointName) {
    Step finalStep = createFinalStep(endPipe, checkpointName);
    WorkflowRunner runner = new WorkflowRunner(name, workingDir + "/checkpoints", 1, 0, finalStep);
    return runner;
  }

  private Step createFinalStep(Pipe endPipe, String checkpointName) {
    CascadingActionBuilder builder = new CascadingActionBuilder();

    CascadingAction action = builder.setName(name + ":" + checkpointName)
        .setCheckpoint(checkpointName)
        .addInputStore(checkpointStore)
        .addOutputStores(outputs)
        .addSource(currentPipe.getName(), checkpointStore.getTap())
        .setSinks(sinks)
        .addTail(endPipe)
        .build();

    return new Step(action, previousStep);
  }

  public Pipe addCheckpoint(Pipe endPipe, String nextPipeName, String checkpointName) {
    Pipe tail = getTail(endPipe);
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

  private Pipe getTail(Pipe endPipe) {
    Pipe tail;
    if (endPipe instanceof SubAssembly) {
      Pipe[] tails = ((SubAssembly) endPipe).getTails();
      if (tails.length == 1) {
        tail = tails[0];
      } else {
        throw new IllegalArgumentException("Cannot infer fields for multi-tail subassemblies");
      }
    } else {
      tail = endPipe;
    }
    return tail;
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

  private class GenericMultiStepAction extends MultiStepAction {

    public GenericMultiStepAction(String checkpointToken, Step tail) {
      super(checkpointToken);

      setSubStepsFromTail(tail);
    }
  }

}
