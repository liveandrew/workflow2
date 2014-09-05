package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.thrift.TBase;

import cascading.flow.FlowListener;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.rapleaf.cascading_ext.datastore.BucketDataStore;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;
import com.rapleaf.cascading_ext.msj_tap.joiner.TOutputMultiJoiner;

public class CascadingAction2 extends MultiStepAction {

  private CascadingWorkflowBuilder workflowHelper;

  public CascadingAction2(String checkpointToken, String tmpRoot) {
    this(checkpointToken, tmpRoot, Maps.<Object, Object>newHashMap());
  }

  public CascadingAction2(String checkpointToken, String tmpRoot, Map<Object, Object> flowProperties) {
    super(checkpointToken, tmpRoot);
    workflowHelper = new CascadingWorkflowBuilder(getTmpRoot(), getClass().getSimpleName(), flowProperties);
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

  protected void completePartitioned(String stepName, Pipe output, BucketDataStore outputStore){
    setSubStepsFromTail(workflowHelper.buildPartitionedTail(stepName, output, outputStore));
  }

  protected void complete(String stepName, Pipe output, DataStore outputStore, TupleDataStore persistStatsStore) throws IOException {
    setSubStepsFromTail(workflowHelper.buildTail(stepName, output, outputStore, persistStatsStore));
  }

  protected Pipe bindSource(String name, DataStore input, TapFactory sourceTap) {
    return workflowHelper.bindSource(name, input, sourceTap);
  }

  protected Pipe bindSource(String name, DataStore input) {
    return workflowHelper.bindSource(name, input);
  }

  protected Pipe bindSource(String name, List<? extends DataStore> inputs) {
    return workflowHelper.bindSource(name, inputs);
  }

  protected Pipe bindSource(String name, List<? extends DataStore> inputs, TapFactory sourceTap) {
    return workflowHelper.bindSource(name, inputs, sourceTap);
  }

  protected <T extends Comparable, O extends TBase> Pipe msj(String name, List<MSJBinding<T>> bindings, TOutputMultiJoiner<T, O> joiner) throws IOException {
    return workflowHelper.msj(name, bindings, joiner);
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
