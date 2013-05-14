package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.Flow;
import cascading.flow.planner.Scope;
import cascading.pipe.OperatorException;
import cascading.pipe.Pipe;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;
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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EasyWorkflow {

  private Map<String, Tap> initialSources = Maps.newHashMap();
  private String workingDir;
  private Map<String, Tap> sinks = Maps.newHashMap();
  private DataStoreBuilder dsBuilder;
  private List<DataStore> inputs;
  private List<DataStore> outputs;
  private Map<String, Step> pipenameToStep;
  private Map<String, TupleDataStore> pipenameToStore;
  private Map<String, String> tailPipeNameToCheckpoint;
  private String name;
  private Map<Object, Object> flowProperties;
  private int checkpoint = 0;
  private List<Flow> childFlows;

  private static final Logger LOG = Logger.getLogger(EasyWorkflow.class);

  private EasyWorkflow(String name, String workingDir) {
    this.workingDir = workingDir;
    this.dsBuilder = new DataStoreBuilder(workingDir);
    this.inputs = Lists.newArrayList();
    this.outputs = Lists.newArrayList();
    this.sinks = Maps.newHashMap();
    this.initialSources = Maps.newHashMap();
    this.name = name;
    this.flowProperties = Maps.newHashMap();
    this.pipenameToStep = Maps.newHashMap();
    this.pipenameToStore = Maps.newHashMap();
    this.tailPipeNameToCheckpoint = Maps.newHashMap();
    this.childFlows = Lists.newArrayList();
  }

  private EasyWorkflow(String name, String workingDir, Map<String, Tap> sources, Map<String, Tap> sinks) {
    this(name, workingDir);
    this.initialSources = sources;
    this.sinks = sinks;
  }

  public static EasyWorkflow create(String name, String workingDir, Map<String, Tap> sources, Map<String, Tap> sinks) {
    return new EasyWorkflow(name, workingDir, sources, sinks);
  }

  public static EasyWorkflow create(String name, String workingDir) {
    return new EasyWorkflow(name, workingDir);
  }

  public EasyWorkflow setSources(Map<String, Tap> sources) {
    this.initialSources = sources;
    return this;
  }

  public EasyWorkflow setSinks(Map<String, Tap> sinks) {
    this.sinks = sinks;
    return this;
  }

  public EasyWorkflow addToSources(Map<String, Tap> sources) {
    this.initialSources.putAll(sources);
    return this;
  }

  public EasyWorkflow addToSinks(Map<String, Tap> sinks) {
    this.sinks.putAll(sinks);
    return this;
  }

  public EasyWorkflow addSourceTap(String pipeName, Tap tap) {
    initialSources.put(pipeName, tap);
    return this;
  }

  public EasyWorkflow addSinkTap(String pipeName, Tap tap) {
    sinks.put(pipeName, tap);
    return this;
  }

  public EasyWorkflow setInputs(List<DataStore> inputs) {
    this.inputs = inputs;
    return this;
  }

  public EasyWorkflow addInput(DataStore input) {
    this.inputs.add(input);
    return this;
  }

  public EasyWorkflow addOutput(DataStore output) {
    this.outputs.add(output);
    return this;
  }

  public EasyWorkflow setOutputs(List<DataStore> outputs) {
    this.outputs = outputs;
    return this;
  }

  public Step completeAsStep(String checkpointName, Pipe... endPipes) {
    return completeAsStep(checkpointName, null, endPipes);
  }

  public Step completeAsStep(String checkpointName, FlowCompletedCallback flowCompletedCallback, Pipe... endPipes) {
    Step finalStep = createFinalStep(checkpointName, flowCompletedCallback, endPipes);
    return finalStep;
  }

  public MultiStepAction completeAsMultiStepAction(String checkpointName, Pipe... endPipes) {
    return completeAsMultiStepAction(checkpointName, null, endPipes);
  }

  public MultiStepAction completeAsMultiStepAction(String checkpointName, FlowCompletedCallback flowCompletedCallback, Pipe... endPipes) {
    Step finalStep = createFinalStep(checkpointName, flowCompletedCallback,endPipes);
    MultiStepAction action = new GenericMultiStepAction(name, finalStep);
    return action;
  }

  public WorkflowRunner completeAsWorkflow(String checkpointName, Pipe... endPipes) {
    return completeAsWorkflow(checkpointName, null, endPipes);
  }

  public WorkflowRunner completeAsWorkflow(String checkpointName, FlowCompletedCallback flowCompletedCallback, Pipe... endPipes) {
    Step finalStep = createFinalStep(checkpointName, flowCompletedCallback, endPipes);
    WorkflowRunner runner = new WorkflowRunner(name, workingDir + "/checkpoints", 1, 0, finalStep);
    return runner;
  }

  private Step createFinalStep(String checkpointName, FlowCompletedCallback flowCompletedCallback, Pipe... endPipes) {
    CascadingActionBuilder builder = new CascadingActionBuilder();
    Map<String, Tap> sources = Maps.newHashMap();
    Set<DataStore> inputs = Sets.newHashSet();
    List<Step> previousSteps = Lists.newArrayList();
    for (Pipe endPipe : endPipes) {
      sources.putAll(createSourceMap(endPipe.getHeads()));
      inputs.addAll(getInputStores(endPipe.getHeads()));
      previousSteps.addAll(getPreviousSteps(endPipe.getHeads()));
    }

    CascadingAction action = builder.setName(name + ":" + checkpointName)
        .setCheckpoint(checkpointName)
        .addInputStores(Lists.newArrayList(inputs))
        .addOutputStores(outputs)
        .setSources(sources)
        .setSinks(sinks)
        .addTails(endPipes)
        .setFlowCompletedCallback(flowCompletedCallback)
        .build();

    childFlows.add(action.getFlow());

    analyze();

    return new Step(action, previousSteps);
  }

  private void analyze() {
    StringBuilder logMessage = new StringBuilder("\n==================================\n");
    logMessage.append("Your workflow will require " + childFlows.size() + " actions\n");
    int numSteps = 0;
    int maps = 0;
    int reduces = 0;
    for (Flow flow : childFlows) {
      numSteps += flow.getFlowStats().getStepsCount();
      for (FlowStepStats flowStepStats : flow.getFlowStats().getFlowStepStats()) {
        if (flowStepStats instanceof HadoopStepStats) {
          maps += ((HadoopStepStats) flowStepStats).getNumMapTasks();
          reduces += ((HadoopStepStats) flowStepStats).getNumReduceTasks();
        }
      }
    }
    logMessage.append("Your workflow will require " + numSteps + " mapreduce jobs\n");
    logMessage.append("Your workflow will require " + maps + " map tasks\n");
    logMessage.append("Your workflow will require " + reduces + " reduce tasks\n");

    logMessage.append("==================================");
    LOG.info(logMessage.toString());
  }

  private List<Step> getPreviousSteps(Pipe[] heads) {
    List<Step> output = Lists.newArrayList();
    for (Pipe head : heads) {
      if (pipenameToStep.containsKey(head.getName())) {
        output.add(pipenameToStep.get(head.getName()));
      }
    }
    return output;
  }

  private Map<String, Tap> createSourceMap(Pipe[] heads) {
    Map<String, Tap> output = Maps.newHashMap();
    for (Pipe head : heads) {
      if (initialSources.containsKey(head.getName())) {
        output.put(head.getName(), initialSources.get(head.getName()));
      } else if (pipenameToStore.containsKey(head.getName())) {
        output.put(head.getName(), pipenameToStore.get(head.getName()).getTap());
      } else {
        throw new RuntimeException("Could not find tap for head pipe named " + head.getName());
      }
    }
    return output;
  }

  private Fields determineOutputFields(Pipe tail) {
    return getScope(tail).getIncomingTapFields();
  }

  private Scope getScope(Pipe tail) {
    Pipe[] previousPipes = tail.getPrevious();
    if (previousPipes.length == 0) {
      Fields sourceFields;
      if (initialSources.containsKey(tail.getName())) {
        sourceFields = initialSources.get(tail.getName()).getSourceFields();
      } else if (pipenameToStore.containsKey(tail.getName())) {
        sourceFields = pipenameToStore.get(tail.getName()).getTap().getSourceFields();
      } else {
        throw new RuntimeException("Cannot find head pipe name " + tail.getName() + " in any source map during field resolution");
      }
      Scope scope = new Scope(sourceFields);
      scope.setName(tail.getName());
      return scope;
    } else {
      Set<Scope> scopes = Sets.newHashSet();
      for (Pipe previous : previousPipes) {
        scopes.add(getScope(previous));
      }
      try {
        return tail.outgoingScopeFor(scopes);
      }
      catch (OperatorException e) {
        throw e;
      }
    }
  }

  public Pipe addCheckpoint(Pipe endPipe, Fields fields) {
    return addCheckpoint(endPipe, "check" + checkpoint, fields, null);
  }

  public Pipe addCheckpoint(Pipe endPipe, Fields fields, FlowCompletedCallback flowCompletedCallback) {
    return addCheckpoint(endPipe, "check" + checkpoint, fields, flowCompletedCallback);
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName, Fields fields) {
    return addCheckpoint(endPipe, checkpointName, fields, null);
  }

  public Pipe addCheckpoint(Pipe endPipe) {
    return addCheckpoint(endPipe, "check" + checkpoint, determineOutputFields(endPipe), null);
  }

  public Pipe addCheckpoint(Pipe endPipe, FlowCompletedCallback flowCompletedCallback) {
    return addCheckpoint(endPipe, "check" + checkpoint, determineOutputFields(endPipe), flowCompletedCallback);
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName) {
    return addCheckpoint(endPipe, checkpointName, determineOutputFields(endPipe), null);
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName, FlowCompletedCallback flowCompletedCallback) {
    return addCheckpoint(endPipe, checkpointName, determineOutputFields(endPipe), flowCompletedCallback);
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName, Fields fields, FlowCompletedCallback flowCompletedCallback) {
    try {
      CascadingActionBuilder builder = new CascadingActionBuilder();
      LOG.info("determined output fields to be " + fields + " for step " + checkpointName);
      TupleDataStore checkpointStore = dsBuilder.getTupleDataStore(checkpointName, fields);

      Pipe[] heads = endPipe.getHeads();
      Map<String, Tap> sources = createSourceMap(heads);
      Set<DataStore> inputStores = getInputStores(heads);

      CascadingAction action = builder.setName(name + ":" + checkpointName)
          .setCheckpoint(checkpointName)
          .addInputStores(Lists.newArrayList(inputStores))
          .addOutputStore(checkpointStore)
          .setSources(sources)
          .addSink(endPipe.getName(), checkpointStore.getTap())
          .addTail(endPipe)
          .addFlowProperties(flowProperties)
          .setFlowCompletedCallback(flowCompletedCallback)
          .build();

      childFlows.add(action.getFlow());

      Step step = new Step(action, getPreviousSteps(heads));
      String nextPipeName = "tail-" + checkpointName;

      pipenameToStore.put(nextPipeName, checkpointStore);
      pipenameToStep.put(nextPipeName, step);
      tailPipeNameToCheckpoint.put(nextPipeName, checkpointName);

      Pipe pipe = new Pipe(nextPipeName);
      checkpoint++;
      return pipe;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Set<DataStore> getInputStores(Pipe[] heads) {
    Set<DataStore> inputStores = Sets.newHashSet();
    for (Pipe head : heads) {
      if (initialSources.containsKey(head.getName())) {
        inputStores.addAll(this.inputs);
      } else if (pipenameToStore.containsKey(head.getName())) {
        inputStores.add(pipenameToStore.get(head.getName()));
      } else {
        throw new RuntimeException("Could not find store for head pipe " + head.getName());
      }
    }
    return inputStores;
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

