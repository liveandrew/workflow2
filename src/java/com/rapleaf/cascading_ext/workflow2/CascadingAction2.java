package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cascading.flow.FlowListener;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.liveramp.cascading_tools.EmptyListener;
import com.rapleaf.cascading_ext.HRap;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.msj_tap.store.PartitionableDataStore;
import com.rapleaf.cascading_ext.pipe.PipeFactory;
import com.rapleaf.cascading_ext.tap.TapFactory;
import com.rapleaf.cascading_ext.tap.bucket2.PartitionStructure;

public class CascadingAction2 extends MultiStepAction {

  private CascadingWorkflowBuilder workflowHelper;

  public CascadingAction2(String checkpointToken, String tmpRoot) {
    this(checkpointToken, tmpRoot, Maps.<Object, Object>newHashMap());
  }

  public CascadingAction2(String checkpointToken, String tmpRoot, Map<Object, Object> flowProperties) {
    super(checkpointToken, tmpRoot);
    workflowHelper = new CascadingWorkflowBuilder(getTmpRoot(), getClass().getSimpleName(), flowProperties, false);
  }

  //  TODO kill this once we figure out the cascading internal NPE (upgrade past 2.5.1 maybe?)
  public CascadingAction2(String checkpointToken, String tmpRoot, Map<Object, Object> flowProperties, boolean skipCompleteListener) {
    super(checkpointToken, tmpRoot);
    workflowHelper = new CascadingWorkflowBuilder(getTmpRoot(), getClass().getSimpleName(), flowProperties, skipCompleteListener);
  }

  protected void complete(String stepName, SinkBinding sinkBinding) {
    complete(stepName, Lists.newArrayList(sinkBinding));
  }

  protected void complete(String stepName, List<? extends SinkBinding> sinkBindings) {
    setSubStepsFromTail(workflowHelper.buildTail(stepName, sinkBindings, new EmptyListener()));
  }

  protected void complete(String stepName, List<? extends SinkBinding> sinkBindings, FlowListener listener) {
    setSubStepsFromTail(workflowHelper.buildTail(stepName, sinkBindings, listener));
  }

  protected void complete(String stepName, Pipe output, DataStore outputStore, FlowListener listener) {
    setSubStepsFromTail(workflowHelper.buildTail(stepName, output, outputStore, listener));
  }

  protected void complete(String stepName, Pipe output, DataStore outputStore) {
    setSubStepsFromTail(workflowHelper.buildTail(stepName, output, outputStore));
  }

  protected void completeToNull(Pipe output) {
    setSubStepsFromTail(workflowHelper.buildNullTail(output));
  }

  protected void completePartitioned(String stepName, Pipe output, PartitionableDataStore outputStore, PartitionStructure structure) {
    setSubStepsFromTail(workflowHelper.buildPartitionedTail(stepName, output, outputStore, new PartitionFactory.Now(structure)));
  }

  protected void completePartitioned(String stepName, Pipe output, PartitionableDataStore outputStore, PartitionFactory structure) {
    setSubStepsFromTail(workflowHelper.buildPartitionedTail(stepName, output, outputStore, structure));
  }

  protected Pipe bindSource(String name, DataStore input, TapFactory sourceTap) {
    return bindSource(name, input, sourceTap, new ActionCallback.Default());
  }

  protected Pipe bindSource(String name, DataStore input) {
    return bindSource(name, input, new ActionCallback.Default());
  }

  protected Pipe bindSource(String name, DataStore input, ActionCallback callback) {
    return bindSource(name, input, new TapFactory.SimpleFactory(input), callback);
  }

  protected Pipe bindSource(String name, final Collection<? extends DataStore> inputs) {
    return bindSource(name, inputs, new TapFactory() {
      @Override
      public Tap createTap() {
        return HRap.getMultiTap(inputs);
      }
    }, new ActionCallback.Default());
  }

  protected Pipe bindSource(String name, DataStore input, TapFactory sourceTap, ActionCallback callback) {
    return bindSource(name, Lists.newArrayList(input), sourceTap, callback);
  }

  protected Pipe bindSource(String name, Collection<? extends DataStore> inputs, TapFactory sourceTap) {
    return bindSource(name, inputs, sourceTap, new ActionCallback.Default());
  }

  protected Pipe bindSource(String name, Collection<? extends DataStore> inputs, TapFactory sourceTap, ActionCallback callback) {
    return bindSource(name, new SourceStoreBinding(inputs, sourceTap, new PipeFactory.Fresh()), callback);
  }

  protected Pipe bindSource(String name, SourceStoreBinding sourceStoreBinding) {
    return bindSource(name, sourceStoreBinding, new ActionCallback.Default());
  }

  /** Analogous to {@link #bindSource(String, Collection)} but for
   * {@link SourceStoreBinding}s. We can't use a
   * {@link cascading.tap.MultiSourceTap} since that forces all
   * {@link cascading.scheme.Scheme}s to be the same for every tap.
   *
   * {@link com.rapleaf.cascading_ext.tap.bucket2.BucketTap2} doesn't strictly
   * handle {@link Tap#equals(Object)} so {@link cascading.tap.MultiSourceTap}
   * will make it lose some behaviour.
   * */
  protected Pipe bindSources(String name, Collection<? extends SourceStoreBinding> bindings) {
    int i = 0;
    Pipe[] pipes = new Pipe[bindings.size()];
    for (SourceStoreBinding binding : bindings) {
      pipes[i] = bindSource(name + i, binding);
      i++;
    }

    return new Merge(name, pipes);
  }

  protected Pipe bindSource(String name, SourceStoreBinding sourceStoreBinding, ActionCallback callback) {
    return workflowHelper.bindSource(name, sourceStoreBinding, callback);
  }


  protected Pipe addCheckpoint(Pipe pipe, String checkpointName) throws IOException {
    return workflowHelper.addCheckpoint(pipe, checkpointName);
  }

  protected Pipe addCheckpoint(Pipe pipe) throws IOException {
    return workflowHelper.addCheckpoint(pipe);
  }

  protected Pipe addCheckpoint(Pipe pipe, String checkpointName, FlowListener flowListener) throws IOException {
    return workflowHelper.addCheckpoint(pipe, checkpointName, flowListener);
  }

  protected Pipe addCheckpoint(Pipe pipe, FlowListener flowListener) throws IOException {
    return workflowHelper.addCheckpoint(pipe, flowListener);
  }

  protected Pipe addCheckpoint(Pipe pipe, String checkpointName, Fields fields) throws IOException {
    return workflowHelper.addCheckpoint(pipe, checkpointName, fields);
  }

  protected Pipe addCheckpoint(Pipe pipe, Fields fields) throws IOException {
    return workflowHelper.addCheckpoint(pipe, fields);
  }

  protected Pipe addCheckpoint(Pipe pipe, String checkpointName, Fields fields, FlowListener flowListener) throws IOException {
    return workflowHelper.addCheckpoint(pipe, checkpointName, fields, flowListener);
  }

  protected Pipe addCheckpoint(Pipe pipe, Fields fields, FlowListener flowListener) throws IOException {
    return workflowHelper.addCheckpoint(pipe, fields, flowListener);
  }

}
