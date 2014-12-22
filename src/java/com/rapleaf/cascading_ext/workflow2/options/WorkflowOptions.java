package com.rapleaf.cascading_ext.workflow2.options;

import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnerNotification;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnerNotificationSet;
import com.rapleaf.cascading_ext.workflow2.registry.WorkflowRegistry;
import com.rapleaf.cascading_ext.workflow2.stats.RecorderFactory;

public class WorkflowOptions<T extends WorkflowOptions<T>> {

  private int maxConcurrentSteps;
  private AlertsHandler alertsHandler;
  private Map<Object, Object> workflowJobProperties = Maps.newHashMap();
  private WorkflowRunnerNotificationSet enabledNotifications;
  private RecorderFactory statsRecorder;
  private StoreReaderLockProvider lockProvider;
  private ContextStorage storage;
  private WorkflowRegistry registry;

  protected WorkflowOptions(){}

  public int getMaxConcurrentSteps() {
    return maxConcurrentSteps;
  }

  public T setMaxConcurrentSteps(int maxConcurrentSteps) {
    this.maxConcurrentSteps = maxConcurrentSteps;
    return (T) this;
  }

  public AlertsHandler getAlertsHandler() {
    return alertsHandler;
  }

  public T setAlertsHandler(AlertsHandler alertsHandler) {
    this.alertsHandler = alertsHandler;
    return (T) this;
  }

  public WorkflowRegistry getRegistry() {
    return registry;
  }

  public T setRegistry(WorkflowRegistry registry) {
    this.registry = registry;
    return (T) this;
  }

  public T setEnabledNotifications(WorkflowRunnerNotification enabledNotification,
                                                       WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = WorkflowRunnerNotificationSet.only(enabledNotification, enabledNotifications);
    return (T) this;
  }

  public T setWorkflowJobProperties(Map<Object, Object> properties){
    this.workflowJobProperties = properties;
    return (T) this;
  }

  public T setEnabledNotifications(WorkflowRunnerNotificationSet enabledNotifications) {
    this.enabledNotifications = enabledNotifications;
    return (T) this;
  }

  public T setEnabledNotificationsExcept(WorkflowRunnerNotification enabledNotification,
                                                             WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = WorkflowRunnerNotificationSet.except(enabledNotification, enabledNotifications);
    return (T) this;
  }

  public WorkflowRunnerNotificationSet getEnabledNotifications() {
    return enabledNotifications;
  }


  public Map<Object, Object> getWorkflowJobProperties() {
    return workflowJobProperties;
  }

  public ContextStorage getStorage() {
    return storage;
  }

  public T setStorage(ContextStorage storage) {
    this.storage = storage;
    return (T) this;
  }

  public StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }

  public T setLockProvider(StoreReaderLockProvider lockProvider) {
    this.lockProvider = lockProvider;
    return (T) this;
  }

  public RecorderFactory getStatsRecorder() {
    return statsRecorder;
  }

  public T setStatsRecorder(RecorderFactory statsRecorder) {
    this.statsRecorder = statsRecorder;
    return (T) this;
  }
}
