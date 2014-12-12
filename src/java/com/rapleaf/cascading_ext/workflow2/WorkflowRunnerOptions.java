package com.rapleaf.cascading_ext.workflow2;

import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.NoOpAlertsHandler;

public class WorkflowRunnerOptions {

  private int maxConcurrentSteps;
  private AlertsHandler alertsHandler;
  private Map<Object, Object> workflowJobProperties = Maps.newHashMap();
  private WorkflowRunnerNotificationSet enabledNotifications;
  private int statsDPort;
  private String statsDHost;
  private StoreReaderLockProvider lockProvider;
  private ContextStorage storage;
  private WorkflowRegistry registry;

  public WorkflowRunnerOptions() {
    maxConcurrentSteps = Integer.MAX_VALUE;
    alertsHandler = new NoOpAlertsHandler();
    enabledNotifications = WorkflowRunnerNotificationSet.all();
    statsDPort = 8125;
    statsDHost = "pglibertyc6";
    lockProvider = null;
    this.storage = new ContextStorage.None();
    this.registry = new ZkRegistry();
  }

  public int getMaxConcurrentSteps() {
    return maxConcurrentSteps;
  }

  public WorkflowRunnerOptions setMaxConcurrentSteps(int maxConcurrentSteps) {
    this.maxConcurrentSteps = maxConcurrentSteps;
    return this;
  }

  public AlertsHandler getAlertsHandler() {
    return alertsHandler;
  }

  public WorkflowRunnerOptions setAlertsHandler(AlertsHandler alertsHandler) {
    this.alertsHandler = alertsHandler;
    return this;
  }

  public WorkflowRegistry getRegistry() {
    return registry;
  }

  public void setRegistry(WorkflowRegistry registry) {
    this.registry = registry;
  }

  public WorkflowRunnerOptions setEnabledNotifications(WorkflowRunnerNotification enabledNotification,
      WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = WorkflowRunnerNotificationSet.only(enabledNotification, enabledNotifications);
    return this;
  }

  public WorkflowRunnerOptions setWorkflowJobProperties(Map<Object, Object> properties){
    this.workflowJobProperties = properties;
    return this;
  }

  public WorkflowRunnerOptions setEnabledNotifications(WorkflowRunnerNotificationSet enabledNotifications) {
    this.enabledNotifications = enabledNotifications;
    return this;
  }

  public WorkflowRunnerOptions setEnabledNotificationsExcept(WorkflowRunnerNotification enabledNotification,
      WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = WorkflowRunnerNotificationSet.except(enabledNotification, enabledNotifications);
    return this;
  }

  public WorkflowRunnerNotificationSet getEnabledNotifications() {
    return enabledNotifications;
  }

  public int getStatsDPort() {
    return statsDPort;
  }

  public Map<Object, Object> getWorkflowJobProperties() {
    return workflowJobProperties;
  }

  public WorkflowRunnerOptions  setStatsDPort(int statsDPort) {
    this.statsDPort = statsDPort;
    return this;
  }

  public String getStatsDHost() {
    return statsDHost;
  }

  public WorkflowRunnerOptions  setStatsDHost(String statsDHost) {
    this.statsDHost = statsDHost;
    return this;
  }

<<<<<<< HEAD
    setMaxConcurrentSteps(Integer.MAX_VALUE);
    setAlertsHandler(new LoggingAlertsHandler());
    setEnabledNotifications(WorkflowRunnerNotificationSet.all());
    setStatsRecorder(new RecorderFactory.StatsD());
    setLockProvider(null);
    setStorage(new ContextStorage.None());
    setRegistry(new ZkRegistry());
=======
  public ContextStorage getStorage() {
    return storage;
>>>>>>> parent of 87d02ec... Merge pull request #275 from MasterRepos/pull_out_options
  }

  public WorkflowRunnerOptions setStorage(ContextStorage storage) {
    this.storage = storage;
    return this;
  }

  public StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }

  public WorkflowRunnerOptions  setLockProvider(StoreReaderLockProvider lockProvider) {
    this.lockProvider = lockProvider;
    return this;
  }
}
