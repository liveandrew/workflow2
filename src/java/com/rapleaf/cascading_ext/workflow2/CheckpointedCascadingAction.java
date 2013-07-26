package com.rapleaf.cascading_ext.workflow2;

import java.util.List;
import java.util.Map;

import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.rapleaf.cascading_ext.datastore.DataStore;

public abstract class CheckpointedCascadingAction extends MultiStepAction {

  private EasyWorkflow workflowHelper;
  private String name;

  public CheckpointedCascadingAction(String checkpointToken, String workingDirectory, List<DataStore> inputs, List<DataStore> outputs) {
    super(checkpointToken);
    workflowHelper = EasyWorkflow.create(this.getClass().getSimpleName(), workingDirectory);
    workflowHelper.setInputs(inputs);
    workflowHelper.setOutputs(outputs);
  }

  protected void complete(Pipe pipe, String pipeName) {
    complete(pipe, pipeName, null);
  }

  protected void complete(String pipeName, Pipe... pipes) {
    complete(pipeName, null, pipes);
  }

  protected void complete(Pipe pipe, String pipeName, FlowCompletedCallback flowCompletedCallback) {
    setSubStepsFromTail(workflowHelper.completeAsStep(pipeName, flowCompletedCallback, pipe));
  }

  protected void complete(String pipeName, FlowCompletedCallback flowCompletedCallback, Pipe... pipes) {
    setSubStepsFromTail(workflowHelper.completeAsStep(pipeName, flowCompletedCallback, pipes));
  }

  protected Pipe addCheckpoint(Pipe pipe, String checkpointName) {
    return workflowHelper.addCheckpoint(pipe, checkpointName);
  }

  protected Pipe addCheckpoint(Pipe pipe) {
    return workflowHelper.addCheckpoint(pipe);
  }

  protected Pipe addCheckpoint(Pipe pipe, String checkpointName, FlowCompletedCallback flowCompletedCallback) {
    return workflowHelper.addCheckpoint(pipe, checkpointName, flowCompletedCallback);
  }

  protected Pipe addCheckpoint(Pipe pipe, FlowCompletedCallback flowCompletedCallback) {
    return workflowHelper.addCheckpoint(pipe, flowCompletedCallback);
  }

  protected Pipe addCheckpoint(Pipe pipe, String checkpointName, Fields fields) {
    return workflowHelper.addCheckpoint(pipe, checkpointName, fields);
  }

  protected Pipe addCheckpoint(Pipe pipe, Fields fields) {
    return workflowHelper.addCheckpoint(pipe, fields);
  }

  protected Pipe addCheckpoint(Pipe pipe, String checkpointName, Fields fields, FlowCompletedCallback flowCompletedCallback) {
    return workflowHelper.addCheckpoint(pipe, checkpointName, fields, flowCompletedCallback);
  }

  protected Pipe addCheckpoint(Pipe pipe, Fields fields, FlowCompletedCallback flowCompletedCallback) {
    return workflowHelper.addCheckpoint(pipe, fields, flowCompletedCallback);
  }

  protected void addSourceTap(String name, Tap tap) {
    workflowHelper.addSourceTap(name, tap);
  }

  protected void addSinkTap(String name, Tap tap) {
    workflowHelper.addSinkTap(name, tap);
  }

  public void setName(String name) {
    this.name = name;
  }

  protected void addSourceTaps(Map<String, Tap> sources) {
    for (Map.Entry<String, Tap> source : sources.entrySet()) {
      addSourceTap(source.getKey(), source.getValue());
    }
  }

  protected void addSinkTap(Tap sink) {
    addSinkTap("singleton-sink", sink);
  }

  protected void addSinkTaps(Map<String, Tap> sinks) {
    for (Map.Entry<String, Tap> sink : sinks.entrySet()) {
      addSinkTap(sink.getKey(), sink.getValue());
    }
  }

  protected void addFlowProperties(Map<Object, Object> properties) {
    workflowHelper.addFlowProperties(properties);
  }

  protected void addSourceTap(Tap source) {
    addSourceTap("singleton-source", source);
  }
}
