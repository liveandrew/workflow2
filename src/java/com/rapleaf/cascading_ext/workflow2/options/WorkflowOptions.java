package com.rapleaf.cascading_ext.workflow2.options;

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

public class WorkflowOptions {

  private int maxConcurrentSteps;
  private AlertsHandler alertsHandler;
  private NestedProperties nestedProperties = null;
  private WorkflowRunnerNotificationSet enabledNotifications;
  private StoreReaderLockProvider lockProvider;
  private ContextStorage storage;
  private String uniqueIdentifier;
  private AppType appType;
  private Integer stepPollInterval;
  private CounterFilter counterFilter;

  protected WorkflowOptions(){}

  public int getMaxConcurrentSteps() {
    return maxConcurrentSteps;
  }

  public WorkflowOptions setMaxConcurrentSteps(int maxConcurrentSteps) {
    this.maxConcurrentSteps = maxConcurrentSteps;
    return this;
  }

  public WorkflowOptions setStepPollInterval(int ms){
    this.stepPollInterval = ms;
    return this;
  }

  public Integer getStepPollInterval() {
    return stepPollInterval;
  }

  public AlertsHandler getAlertsHandler() {
    return alertsHandler;
  }

  public WorkflowOptions setAlertsHandler(AlertsHandler alertsHandler) {
    this.alertsHandler = alertsHandler;
    return this;
  }

  public WorkflowOptions setEnabledNotifications(WorkflowRunnerNotification enabledNotification,
                                                       WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = WorkflowRunnerNotificationSet.only(enabledNotification, enabledNotifications);
    return this;
  }

  public WorkflowOptions addWorkflowHadoopProperties(HadoopProperties workflowHadoopProperties) {
    this.nestedProperties = new NestedProperties(this.nestedProperties, workflowHadoopProperties);
    return this;
  }

  public WorkflowOptions setEnabledNotifications(WorkflowRunnerNotificationSet enabledNotifications) {
    this.enabledNotifications = enabledNotifications;
    return this;
  }

  public WorkflowOptions setEnabledNotificationsExcept(WorkflowRunnerNotification enabledNotification,
                                                             WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = WorkflowRunnerNotificationSet.except(enabledNotification, enabledNotifications);
    return this;
  }

  public WorkflowRunnerNotificationSet getEnabledNotifications() {
    return enabledNotifications;
  }


  public NestedProperties getWorkflowJobProperties() {
    return new NestedProperties(nestedProperties, CascadingHelper.get().getDefaultHadoopProperties());
  }

  public ContextStorage getStorage() {
    return storage;
  }

  public WorkflowOptions setStorage(ContextStorage storage) {
    this.storage = storage;
    return this;
  }

  public StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }

  public WorkflowOptions setLockProvider(StoreReaderLockProvider lockProvider) {
    this.lockProvider = lockProvider;
    return this;
  }

  public String getScopeIdentifier() {
    return uniqueIdentifier;
  }

  public WorkflowOptions setUniqueIdentifier(String uniqueIdentifier) {
    this.uniqueIdentifier = uniqueIdentifier;
    return this;
  }

  public AppType getAppType() {
    return appType; 
  }

  public WorkflowOptions setAppType(AppType appType) {
    this.appType = appType;
    return this;
  }

  public CounterFilter getCounterFilter() {
    return counterFilter;
  }

  public WorkflowOptions setCounterFilter(CounterFilter filter) {
    this.counterFilter = filter;
    return this;
  }
}
