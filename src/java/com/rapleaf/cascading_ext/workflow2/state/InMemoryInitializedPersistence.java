package com.rapleaf.cascading_ext.workflow2.state;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_state.InitializedPersistence;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

public class InMemoryInitializedPersistence implements InitializedPersistence {

  private final String name;
  private String priority;
  private String pool;
  private final String host;
  private final String username;
  private final AlertsHandler handler;
  private final Set<WorkflowRunnerNotification> configuredNotifications;

  public InMemoryInitializedPersistence(String name, String priority, String pool, String host, String username, AlertsHandler handler, Set<WorkflowRunnerNotification> configuredNotifications) {
    this.name = name;
    this.priority = priority;
    this.pool = pool;
    this.host = host;
    this.username = username;
    this.handler = handler;
    this.configuredNotifications = configuredNotifications;
  }

  @Override
  public long getExecutionId() throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public long getAttemptId() throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public void stop() throws IOException {
    //  no op
  }

  public String getName() {
    return name;
  }

  public String getPriority() {
    return priority;
  }

  public String getPool() {
    return pool;
  }

  public String getHost() {
    return host;
  }

  public String getUsername() {
    return username;
  }

  public AlertsHandler getHandler() {
    return handler;
  }

  public Set<WorkflowRunnerNotification> getConfiguredNotifications() {
    return configuredNotifications;
  }

  public void setPriority(String priority) {
    this.priority = priority;
  }

  public void setPool(String pool) {
    this.pool = pool;
  }
}
