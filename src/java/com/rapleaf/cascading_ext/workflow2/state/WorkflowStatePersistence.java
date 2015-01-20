package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.rapleaf.cascading_ext.workflow2.Step;

public interface WorkflowStatePersistence {

  public StepState getState(String stepToken);
  public WorkflowState getFlowStatus();

  public void markStepRunning(String stepToken) throws IOException;
  public void markStepFailed(String stepToken, Throwable t) throws IOException;
  public void markStepSkipped(String stepToken) throws IOException;
  public void markStepCompleted(String stepToken) throws IOException;

  public void markStepStatusMessage(String stepToken, String newMessage);

  public void prepare(DirectedGraph<Step, DefaultEdge> flatSteps);

  public void markShutdownRequested(String reason);
  public void markWorkflowStopped();

}
