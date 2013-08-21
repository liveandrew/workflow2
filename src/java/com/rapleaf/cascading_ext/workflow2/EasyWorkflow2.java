package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.flow.planner.Scope;
import cascading.pipe.Pipe;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.workflow2.action.CascadingAction;
import com.rapleaf.cascading_ext.workflow2.action.CascadingActionBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class EasyWorkflow2 {
  private static final Logger LOG = Logger.getLogger(EasyWorkflow2.class);
  private static final int DEFAULT_MAX_CONCURRENCY = 1;

  private String workingDir;

  private DataStoreBuilder dsBuilder;
  private Set<Step> subSteps = Sets.newHashSet();

  private Map<String, DataStore> pipenameToSourceStore; //  store which is the source for the pipe
  private Map<String, Tap> pipenameToTap;               //  tap which provides input
  private Multimap<String, Step> pipenameToParentStep;  //  what step(s) does the pipe depend on

  private String name;
  private Map<Object, Object> flowProperties;
  private int checkpoint = 0;

  public EasyWorkflow2(String name, String workingDir) {
    this(name, workingDir, Maps.newHashMap());
  }

  public EasyWorkflow2(String name, String workingDir, Map<Object, Object> flowProperties) {
    this.workingDir = workingDir;
    this.dsBuilder = new DataStoreBuilder(workingDir+"/temp-stores");
    this.name = name;
    this.flowProperties = Maps.newHashMap(flowProperties);
    this.pipenameToParentStep = HashMultimap.create();
    this.pipenameToSourceStore = Maps.newHashMap();
    this.pipenameToTap = Maps.newHashMap();
  }

  //  bind source and sink taps to the flow.  This will set the creates / readsFrom datastores as well as
  //  create the necessary actions

  public Pipe bindSource(String name, DataStore source) {
    return bindSource(name, source, source.getTap());
  }

  public Pipe bindSource(String name, DataStore source, Tap tap) {
    pipenameToSourceStore.put(name, source);
    pipenameToTap.put(name, tap);
    return new Pipe(name);
  }

  public void bindSink(String stepName, Pipe output, DataStore outputStore) {
    bindSink(stepName, output, outputStore, new EmptyListener());
  }

  public void bindSink(String stepName, Pipe output, DataStore outputStore, FlowListener callback) {
    subSteps.add(completeFlows(stepName, Lists.newArrayList(output), Collections.singletonMap(stepName, outputStore), callback));
  }

  //  force a persistent checkpoint

  public Pipe addCheckpoint(Pipe endPipe, Fields fields) throws IOException {
    return addCheckpoint(endPipe, "check" + checkpoint, fields, new EmptyListener());
  }

  public Pipe addCheckpoint(Pipe endPipe, Fields fields, FlowListener flowCompletedCallback) throws IOException {
    return addCheckpoint(endPipe, "check" + checkpoint, fields, flowCompletedCallback);
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName, Fields fields) throws IOException {
    return addCheckpoint(endPipe, checkpointName, fields, new EmptyListener());
  }

  public Pipe addCheckpoint(Pipe endPipe) throws IOException {
    return addCheckpoint(endPipe, "check" + checkpoint, determineOutputFields(endPipe), new EmptyListener());
  }

  public Pipe addCheckpoint(Pipe endPipe, FlowListener flowCompletedCallback) throws IOException {
    return addCheckpoint(endPipe, "check" + checkpoint, determineOutputFields(endPipe), flowCompletedCallback);
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName) throws IOException {
    return addCheckpoint(endPipe, checkpointName, determineOutputFields(endPipe), new EmptyListener());
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName, FlowListener flowListener) throws IOException {
    return addCheckpoint(endPipe, checkpointName, determineOutputFields(endPipe), flowListener);
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName, Fields fields, FlowListener flowListener) throws IOException {
    LOG.info("determined output fields to be " + fields + " for step " + checkpointName);
    TupleDataStore checkpointStore = dsBuilder.getTupleDataStore(checkpointName, fields);

    Step step = completeFlows(checkpointName, Lists.newArrayList(endPipe),
        Collections.<String, DataStore>singletonMap(endPipe.getName(), checkpointStore), flowListener);

    String nextPipeName = "tail-" + checkpointName;

    subSteps.add(step);
    pipenameToParentStep.put(nextPipeName, step);
    pipenameToSourceStore.put(nextPipeName, checkpointStore);
    pipenameToTap.put(nextPipeName, checkpointStore.getTap());

    Pipe pipe = new Pipe(nextPipeName);
    checkpoint++;
    return pipe;
  }


  //  build the workflow or multi step action

  public WorkflowRunner buildWorkflow() {
    return buildWorkflow(new WorkflowRunnerOptions().setMaxConcurrentSteps(DEFAULT_MAX_CONCURRENCY));
  }

  public WorkflowRunner buildWorkflow(WorkflowRunnerOptions options){
    analyze();
    return new WorkflowRunner(name, workingDir + "/checkpoints", options, subSteps);
  }

  public Step buildStep(String stepName) {
    analyze();
    return new Step(new MultiStepAction(stepName, subSteps));
  }


  //  internal stuff

  private Step completeFlows(String name, List<Pipe> endPipes, Map<String, DataStore> sinkStores, FlowListener flowListener) {
    CascadingActionBuilder builder = new CascadingActionBuilder();
    Map<String, Tap> sources = Maps.newHashMap();
    Map<String, Tap> sinks = Maps.newHashMap();

    Set<DataStore> inputs = Sets.newHashSet();
    List<Step> previousSteps = Lists.newArrayList();

    for (Entry<String, DataStore> entry : sinkStores.entrySet()) {
      sinks.put(entry.getKey(), entry.getValue().getTap());
    }

    for (Pipe endPipe : endPipes) {
      sources.putAll(createSourceMap(endPipe.getHeads()));
      inputs.addAll(getInputStores(endPipe.getHeads()));
      previousSteps.addAll(getPreviousSteps(endPipe.getHeads()));
    }

    CascadingAction action = builder.setName(name + ":" + name)
        .setCheckpoint(name)
        .addInputStores(Lists.newArrayList(inputs))
        .addOutputStores(Lists.newArrayList(sinkStores.values()))
        .setSources(sources)
        .setSinks(sinks)
        .addTails(endPipes.toArray(new Pipe[endPipes.size()]))
        .addFlowProperties(flowProperties)
        .setFlowListener(flowListener)
        .build();

    return new Step(action, previousSteps);
  }

  private void analyze() {

    StringBuilder logMessage = new StringBuilder("\n==================================\n");
    logMessage.append("Your workflow will require ").append(subSteps.size()).append(" actions\n");
    int numSteps = 0;
    int maps = 0;
    int reduces = 0;

    for (Step subStep : subSteps) {
      if (subStep.getAction() instanceof CascadingAction) {
        Flow flow = ((CascadingAction) subStep.getAction()).getFlow();
        numSteps += flow.getFlowStats().getStepsCount();
        for (FlowStepStats flowStepStats : flow.getFlowStats().getFlowStepStats()) {
          if (flowStepStats instanceof HadoopStepStats) {
            maps += ((HadoopStepStats) flowStepStats).getNumMapTasks();
            reduces += ((HadoopStepStats) flowStepStats).getNumReduceTasks();
          }
        }
      }
    }

    logMessage.append("Your workflow will require ").append(numSteps).append(" mapreduce jobs\n");
    logMessage.append("Your workflow will require ").append(maps).append(" map tasks\n");
    logMessage.append("Your workflow will require ").append(reduces).append(" reduce tasks\n");

    logMessage.append("==================================");
    LOG.info(logMessage.toString());
  }

  private List<Step> getPreviousSteps(Pipe[] heads) {
    List<Step> output = Lists.newArrayList();
    for (Pipe head : heads) {
      if (pipenameToParentStep.containsKey(head.getName())) {
        output.addAll(pipenameToParentStep.get(head.getName()));
      }
    }
    return output;
  }

  private Map<String, Tap> createSourceMap(Pipe[] heads) {
    Map<String, Tap> output = Maps.newHashMap();
    for (Pipe head : heads) {
      if (pipenameToTap.containsKey(head.getName())) {
        output.put(head.getName(), pipenameToTap.get(head.getName()));
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
      if (pipenameToTap.containsKey(tail.getName())) {
        sourceFields = pipenameToTap.get(tail.getName()).getSourceFields();
      } else {
        throw new RuntimeException("Cannot find head pipe name " + tail.getName() + " in any source map during field resolution");
      }
      Scope scope = new Scope(sourceFields);
      scope.setName(tail.getName());
      return scope;
    } else {
      Set<Scope> scopes = Sets.newHashSet();
      for (Pipe previous : previousPipes) {
        Scope scope = getScope(previous);
        scope.setName(previous.getName());
        scopes.add(scope);
      }
      return tail.outgoingScopeFor(scopes);
    }
  }

  private Set<DataStore> getInputStores(Pipe[] heads) {
    Set<DataStore> inputStores = Sets.newHashSet();
    for (Pipe head : heads) {
      if (pipenameToSourceStore.containsKey(head.getName())) {
        inputStores.add(pipenameToSourceStore.get(head.getName()));
      } else {
        throw new RuntimeException("Could not find store for head pipe " + head.getName());
      }
    }
    return inputStores;
  }
}

