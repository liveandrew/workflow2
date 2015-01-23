package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.RunningJob;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.importer.generated.AppType;
import com.rapleaf.cascading_ext.workflow2.Step;

public interface WorkflowStatePersistence {

  public void prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                      String name,
                      String scopeId,
                      AppType appType,
                      String host,
                      String username,
                      String pool,
                      String priority);

  public void markStepRunning(String stepToken) throws IOException;
  public void markStepFailed(String stepToken, Throwable t) throws IOException;
  public void markStepSkipped(String stepToken) throws IOException;
  public void markStepCompleted(String stepToken) throws IOException;

  public void markStepStatusMessage(String stepToken, String newMessage) throws IOException;
  public void markStepRunningJob(String stepToken, RunningJob job) throws IOException;

  public void markPool(String pool) throws IOException;
  public void markPriority(String priority) throws IOException;
  public void markShutdownRequested(String reason) throws IOException;
  public void markWorkflowStopped() throws IOException;

  public StepState getState(String stepToken) throws IOException;
  public Map<String, StepState> getStepStatuses() throws IOException;
  public List<DataStoreInfo> getDatastores() throws IOException;
  public String getShutdownRequest() throws IOException;
  public String getPriority() throws IOException;
  public String getPool() throws IOException;
  public String getName() throws IOException;
  public String getId() throws IOException;
  public String getHost() throws IOException;
  public String getUsername() throws IOException;

}
