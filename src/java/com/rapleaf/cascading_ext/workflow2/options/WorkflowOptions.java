package com.rapleaf.cascading_ext.workflow2.options;

import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.cascading_ext.util.HadoopProperties;
import com.liveramp.cascading_ext.util.NestedProperties;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnerNotification;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnerNotificationSet;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;
import com.rapleaf.cascading_ext.workflow2.registry.WorkflowRegistry;
import com.rapleaf.cascading_ext.workflow2.stats.RecorderFactory;

public class WorkflowOptions<T extends WorkflowOptions<T>> {

  private int maxConcurrentSteps;
  private AlertsHandler alertsHandler;
  private NestedProperties defaultNestedProperties = new NestedProperties(null, CascadingHelper.get().getDefaultHadoopProperties());
  private HadoopProperties workflowHadoopProperties = new HadoopProperties();
  private WorkflowRunnerNotificationSet enabledNotifications;
  private RecorderFactory statsRecorder;
  private StoreReaderLockProvider lockProvider;
  private ContextStorage storage;
  private WorkflowRegistry registry;
  private String uniqueIdentifier;
  private AppType appType;
  private Integer stepPollInterval;
  private CounterFilter counterFilter;

  protected WorkflowOptions(){}

  public int getMaxConcurrentSteps() {
    return maxConcurrentSteps;
  }

  public T setMaxConcurrentSteps(int maxConcurrentSteps) {
    this.maxConcurrentSteps = maxConcurrentSteps;
    return (T) this;
  }

  public T setStepPollInterval(int ms){
    this.stepPollInterval = ms;
    return (T) this;
  }

  public Integer getStepPollInterval() {
    return stepPollInterval;
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

  public T setWorkflowDefaultProperties(NestedProperties defaultProperties) {
    this.defaultNestedProperties = defaultProperties;
    return (T) this;
  }

  public T setWorkflowHadoopProperties(HadoopProperties workflowHadoopProperties) {
    this.workflowHadoopProperties = workflowHadoopProperties;
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


  public NestedProperties getWorkflowJobProperties() {
    return new NestedProperties(defaultNestedProperties, workflowHadoopProperties);
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

  public String getScopeIdentifier() {
    return uniqueIdentifier;
  }

  public T setUniqueIdentifier(String uniqueIdentifier) {
    this.uniqueIdentifier = uniqueIdentifier;
    return (T) this;
  }

  public AppType getAppType() {
    return appType;
  }

  public T setAppType(AppType appType) {
    this.appType = appType;
    return (T) this;
  }

  public CounterFilter getCounterFilter() {
    return counterFilter;
  }

  public T setCounterFilter(CounterFilter filter) {
    this.counterFilter = filter;
    return (T) this;
  }
}
