package com.rapleaf.cascading_ext.workflow2;

import cascading.flow.FlowListener;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.common.collect.Lists;
import com.rapleaf.cascading_ext.datastore.DataStore;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class CheckpointedCascadingAction2 extends MultiStepAction {

  private EasyWorkflow2 workflowHelper;

  public CheckpointedCascadingAction2(String checkpointToken, String tmpRoot, Map<Object, Object> flowProperties) {
    super(checkpointToken, tmpRoot);

    workflowHelper = new EasyWorkflow2(checkpointToken, getTmpRoot(), flowProperties);

    setSubStepsFromTail(workflowHelper.buildStep("checkpointed-flow"));
  }

  protected Pipe bindSource(String name, DataStore input, Tap sourceTap){
    return workflowHelper.bindSource(name, input, sourceTap);
  }

  protected Pipe bindSource(String name, DataStore input){
    return workflowHelper.bindSource(name, input);
  }

  public void bindSink(String stepName, Pipe output, DataStore outputStore) {
    workflowHelper.bindSink(stepName, output, outputStore);
  }

  public void bindSink(String stepName, Pipe output, DataStore outputStore, FlowListener callback) {
    workflowHelper.bindSink(stepName, output, outputStore, callback);
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
