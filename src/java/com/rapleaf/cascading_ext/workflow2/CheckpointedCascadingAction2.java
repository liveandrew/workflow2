package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.FlowListener;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.rapleaf.cascading_ext.datastore.DataStore;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CheckpointedCascadingAction2 extends MultiStepAction {

  private CascadingWorkflowBuilder workflowHelper;

  public CheckpointedCascadingAction2(String checkpointToken, String tmpRoot, Map<Object, Object> flowProperties) {
    super(checkpointToken, tmpRoot);

    workflowHelper = new CascadingWorkflowBuilder(getTmpRoot(), flowProperties);
  }

  protected void complete(String stepName, List<SinkBinding> sinkBindings){
    setSubStepsFromTail(workflowHelper.buildTail(stepName, sinkBindings, new EmptyListener()));
  }

  protected void complete(String stepName, List<SinkBinding> sinkBindings, FlowListener listener){
    setSubStepsFromTail(workflowHelper.buildTail(stepName, sinkBindings, listener));
  }

  protected void complete(String stepName, Pipe output, DataStore outputStore, FlowListener listener){
    setSubStepsFromTail(workflowHelper.buildTail(stepName, output, outputStore, listener));
  }

  protected void complete(String stepName, Pipe output, DataStore outputStore){
    setSubStepsFromTail(workflowHelper.buildTail(stepName, output, outputStore));
  }

  protected Pipe bindSource(String name, DataStore input, Tap sourceTap){
    return workflowHelper.bindSource(name, input, sourceTap);
  }

  protected Pipe bindSource(String name, DataStore input){
    return workflowHelper.bindSource(name, input);
  }

  protected Pipe bindSource(String name, List<DataStore> inputs){
    return workflowHelper.bindSource(name, inputs);
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
