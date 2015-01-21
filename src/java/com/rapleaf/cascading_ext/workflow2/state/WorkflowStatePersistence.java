package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.RunningJob;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.rapleaf.cascading_ext.workflow2.Step;

public interface WorkflowStatePersistence {

  public void prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                      String description,
                      String host,
                      String username,
                      String pool,
                      String priority);

  public void markStepRunning(String stepToken) throws IOException;
  public void markStepFailed(String stepToken, Throwable t) throws IOException;
  public void markStepSkipped(String stepToken) throws IOException;
  public void markStepCompleted(String stepToken) throws IOException;

  public void markStepStatusMessage(String stepToken, String newMessage);
  public void markStepRunningJob(String stepToken, RunningJob job);

  public void markPool(String pool);
  public void markPriority(String priority);
  public void markShutdownRequested(String reason);
  public void markWorkflowStopped();

  public StepState getState(String stepToken);
  public Map<String, StepState> getStepStatuses();
  public List<DataStoreInfo> getDatastores();
  public String getShutdownRequest();
  public String getPriority();
  public String getPool();
  public String getDescription();
  public String getId();
  public String getHost();
  public String getUsername();

}
