package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import cascading.flow.FlowListener;
import cascading.flow.planner.Scope;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.counters.Counter;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.PartitionAssemblySinkBinding;
import com.rapleaf.cascading_ext.datastore.PartitionedDataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.msj_tap.conf.InputConf;
import com.rapleaf.cascading_ext.msj_tap.joiner.TOutputMultiJoiner;
import com.rapleaf.cascading_ext.msj_tap.scheme.MSJScheme;
import com.rapleaf.cascading_ext.msj_tap.scheme.MergingScheme;
import com.rapleaf.cascading_ext.msj_tap.tap.MSJTap;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;
import com.rapleaf.cascading_ext.workflow2.SinkBinding.DSSink;
import com.rapleaf.cascading_ext.workflow2.TapFactory.SimpleFactory;
import com.rapleaf.cascading_ext.workflow2.action.FutureCascadingAction;
import com.rapleaf.formats.test.TupleDataStoreHelper;

public class CascadingWorkflowBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(CascadingWorkflowBuilder.class);

  private final String flowName;
  private final DataStoreBuilder dsBuilder;
  private final Set<Step> subSteps = Sets.newHashSet();

  private Map<String, TapFactory> pipenameToTap;                     //  tap which provides input
  private Multimap<String, DataStore> pipenameToSourceStore;  //  stores which are the source for the pipe
  private Multimap<String, Step> pipenameToParentStep;        //  what step(s) does the pipe depend on

  private Map<Object, Object> flowProperties;
  private int checkpointCount = 0;

  public CascadingWorkflowBuilder(String workingDir, String flowName) {
    this(workingDir, flowName, Maps.newHashMap());
  }

  public CascadingWorkflowBuilder(String workingDir, String flowName, Map<Object, Object> flowProperties) {
    this.dsBuilder = new DataStoreBuilder(workingDir + "/temp-stores");
    this.flowName = flowName;
    this.flowProperties = Maps.newHashMap(flowProperties);
    this.pipenameToParentStep = HashMultimap.create();
    this.pipenameToSourceStore = HashMultimap.create();
    this.pipenameToTap = Maps.newHashMap();
  }

  //  bind source and sink taps to the flow.  This will set the creates / readsFrom datastores as well as
  //  create the necessary actions

  public Pipe bindSource(String name, DataStore source) {
    return bindSource(name, Lists.newArrayList(source), new SimpleFactory(source));
  }

  public Pipe bindSource(String name, final Collection<? extends DataStore> inputs) {
    return bindSource(name, inputs, new TapFactory() {
      @Override
      public Tap createTap() {
        return HRap.getMultiTap(inputs);
      }
    });
  }

  public Pipe bindSource(String name, DataStore source, TapFactory tap) {
    return bindSource(name, Lists.newArrayList(source), tap);
  }

  public Pipe bindSource(String name, Collection<? extends DataStore> sources, TapFactory tap) {
    pipenameToSourceStore.putAll(name, sources);
    pipenameToTap.put(name, tap);
    return new Pipe(name);
  }

  public <T extends Comparable> Pipe msj(final String name, List<MSJBinding<T>> bindings, final TOutputMultiJoiner<T, ?> joiner) throws IOException {

    //  don't really love the instanceof stuff, but we'll just call it a TODO --ben
    final List<SourceMSJBinding<T>> sourceMSJBindings = Lists.newArrayList();
    for (MSJBinding<T> binding : bindings) {

      //  if the MSJ is coming from a cascading flow, bind it to a datastore so the MSJ can read it
      if (binding instanceof FlowMSJBinding) {
        FlowMSJBinding<T> flowBinding = (FlowMSJBinding<T>)binding;

        String stepName = getNextStepName();
        BucketDataStore checkpointStore = dsBuilder.getBucketDataStore(stepName + "-sink", flowBinding.getRecordType());
        Step step = completeFlows(stepName, Lists.newArrayList(new DSSink(flowBinding.getPipe(), checkpointStore)), new EmptyListener());

        sourceMSJBindings.add(new SourceMSJBinding<T>(binding.getExtractor(), checkpointStore));

        subSteps.add(step);
        pipenameToParentStep.put(name, step);
      }

      //  otherwise we can read it directly
      else if (binding instanceof SourceMSJBinding) {
        sourceMSJBindings.add((SourceMSJBinding<T>)binding);
      }

      //  no idea
      else {
        throw new RuntimeException("Unrecognized binding class: " + binding.getClass());
      }

    }

    for (SourceMSJBinding<T> binding : sourceMSJBindings) {
      pipenameToSourceStore.put(name, binding.getStore());
    }

    pipenameToTap.put(name, new TapFactory() {
      @Override
      public Tap createTap() throws IOException {

        final List<InputConf<BytesWritable>> confs = Lists.newArrayList();
        for (SourceMSJBinding<T> binding : sourceMSJBindings) {
          BucketDataStore store = binding.getStore();
          confs.add(store.getInputConf(binding.getExtractor()));
        }

        return new MSJTap<BytesWritable>(confs, (MSJScheme<BytesWritable>) MergingScheme.of(joiner));
      }
    });

    return new Pipe(name);
  }

  //  TODO if a pipe has nothing done to it, just attach it to the subsequent MSJ (to allow MSJing a store which has
  //  just been sorted with a previously MSJ compatible datastore in a single flow.  Likewise, if a MSJ is the last step
  //  in a flow, just sink directly and don't create an empty cascading step

  //  checkpoints

  public Pipe addCheckpoint(Pipe endPipe, Fields fields) throws IOException {
    return addCheckpoint(endPipe, getNextStepName(), fields, new EmptyListener());
  }

  public Pipe addCheckpoint(Pipe endPipe, Fields fields, FlowListener flowCompletedCallback) throws IOException {
    return addCheckpoint(endPipe, getNextStepName(), fields, flowCompletedCallback);
  }

  public Pipe addCheckpoint(Pipe endPipe, String checkpointName, Fields fields) throws IOException {
    return addCheckpoint(endPipe, checkpointName, fields, new EmptyListener());
  }

  public Pipe addCheckpoint(Pipe endPipe) throws IOException {
    return addCheckpoint(endPipe, getNextStepName(), determineOutputFields(endPipe), new EmptyListener());
  }

  public Pipe addCheckpoint(Pipe endPipe, FlowListener flowCompletedCallback) throws IOException {
    return addCheckpoint(endPipe, getNextStepName(), determineOutputFields(endPipe), flowCompletedCallback);
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

    Step step = completeFlows(checkpointName, Lists.newArrayList(new DSSink(endPipe, checkpointStore)), flowListener);

    String nextPipeName = "tail-" + checkpointName;

    subSteps.add(step);
    pipenameToParentStep.put(nextPipeName, step);
    pipenameToSourceStore.put(nextPipeName, checkpointStore);
    pipenameToTap.put(nextPipeName, new SimpleFactory(checkpointStore));

    return new Pipe(nextPipeName);
  }

  //  build the workflow or multi step action

  public Step buildTail(Pipe output, DataStore outputStore) {
    return buildTail(getNextStepName(), output, outputStore);
  }

  public Step buildNullTail(Pipe output) {
    return buildTail(getNextStepName(), Lists.newArrayList(new SinkBinding.EmptySink(output)));
  }

  public Step buildTail(String tailStepName, Pipe output, DataStore outputStore) {
    return buildTail(tailStepName, Lists.newArrayList(new DSSink(output, outputStore)), new EmptyListener());
  }

  public Step buildPartitionedTail(String tailStepName, Pipe output, BucketDataStore outputStore, PartitionStructure structure) {
    return buildTail(tailStepName, Lists.newArrayList(new SinkBinding.PartitionedSink(output, outputStore, structure)), new EmptyListener());
  }

  public Step buildPartitionedTail(String tailStepName, Pipe output, PartitionedDataStore store, final PartitionStructure structure){
    return buildTail(tailStepName, Lists.newArrayList(new PartitionAssemblySinkBinding(store, output, structure)));
  }

  public Step buildTail(String tailStepName, Pipe output, DataStore outputStore, FlowListener listener) {
    return buildTail(tailStepName, Lists.newArrayList(new DSSink(output, outputStore)), listener);
  }

  public Step buildTail(String tailStepName, List<? extends SinkBinding> sinks) {
    return buildTail(tailStepName, sinks, new EmptyListener());
  }

  public Step buildTail(String tailStepName, List<? extends SinkBinding> sinks, FlowListener listener) {
    Step tail = completeFlows(tailStepName, sinks, listener);

    List<Step> steps = Lists.newArrayList(subSteps);
    steps.add(tail);

    return tail;
  }

  public Step buildTail(String stepName, Pipe output, DataStore outputStore, TupleDataStore persistStatsStore) throws IOException {
    Step tail = buildTail(stepName, output, outputStore);

    return new Step(new PersistStats("persist_stats", persistStatsStore, tail.getCounters()), tail);
  }

  //  internal stuff

  private static class PersistStats extends Action {
    private TupleDataStore outputStats;
    private List<NestedCounter> counters;

    public PersistStats(String checkpointToken, TupleDataStore outputStats, List<NestedCounter> counters) {
      super(checkpointToken);
      this.outputStats = outputStats;
      this.counters = counters;

      creates(outputStats);
    }

    @Override
    protected void execute() throws Exception {
      List<Tuple> tuples = Lists.newArrayList();
      for (NestedCounter counter1 : counters) {
        Counter counter = counter1.getCounter();
        Tuple tuple = new Tuple(counter.getGroup(), counter.getName(), counter.getValue().toString());
        tuples.add(tuple);
      }
      TupleDataStoreHelper.writeToStore(outputStats, tuples);
    }
  }

  private String getNextStepName() {
    return "step-" + (checkpointCount++);
  }

  private Step completeFlows(String checkpointName, List<? extends SinkBinding> sinkBindings, FlowListener flowListener) {
    Map<String, TapFactory> sources = Maps.newHashMap();
    Map<String, TapFactory> sinks = Maps.newHashMap();
    List<DataStore> sinkStores = Lists.newArrayList();

    Set<DataStore> inputs = Sets.newHashSet();
    List<Step> previousSteps = Lists.newArrayList();
    List<Pipe> pipes = Lists.newArrayList();

    for (SinkBinding sinkBinding : sinkBindings) {
      Pipe pipe = sinkBinding.getPipe();
      Pipe[] heads = pipe.getHeads();

      String pipeName = pipe.getName();
      if (sinks.containsKey(pipeName)) {
        throw new RuntimeException("Pipe with name " + pipeName + " already exists!");
      }

      sources.putAll(createSourceMap(heads));
      inputs.addAll(getInputStores(heads));
      previousSteps.addAll(getPreviousSteps(heads));
      pipes.add(pipe);

      sinks.put(pipeName, sinkBinding.getTapFactory());

      //  if it's a dssink, mark as creates
      if (sinkBinding instanceof SinkBinding.DataStoreSink) {
        SinkBinding.DataStoreSink sink = (SinkBinding.DataStoreSink)sinkBinding;
        sinkStores.add(sink.getOutputStore());
      }

    }

    FutureCascadingAction action = new FutureCascadingAction(
        checkpointName,
        flowName,
        sources,
        sinks,
        pipes,
        inputs,
        sinkStores,
        flowProperties,
        flowListener
    );

    return new Step(action, previousSteps);
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

  private Map<String, TapFactory> createSourceMap(Pipe[] heads) {
    Map<String, TapFactory> output = Maps.newHashMap();
    for (Pipe head : heads) {
      if (pipenameToTap.containsKey(head.getName())) {
        output.put(head.getName(), pipenameToTap.get(head.getName()));
      } else {
        throw new RuntimeException("Could not find tap for head pipe named " + head.getName());
      }
    }
    return output;
  }

  private Fields determineOutputFields(Pipe tail) throws IOException {
    return getScope(tail).getIncomingTapFields();
  }

  private Scope getScope(Pipe tail) throws IOException {
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
        inputStores.addAll(pipenameToSourceStore.get(head.getName()));
      } else {
        throw new RuntimeException("Could not find store for head pipe " + head.getName());
      }
    }
    return inputStores;
  }
}

