package com.rapleaf.cascading_ext.workflow2.options;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.util.HadoopProperties;
import com.liveramp.cascading_tools.properties.PropertiesUtil;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.TrackerURLBuilder;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;

public class WorkflowOptions {

  private int maxConcurrentSteps;
  private AlertsHandler alertsHandler;
  private HadoopProperties properties = new HadoopProperties(Maps.newHashMap(), false);
  private Set<WorkflowRunnerNotification> enabledNotifications;
  private StoreReaderLockProvider lockProvider;
  private ContextStorage storage;
  private String uniqueIdentifier;
  private AppType appType;
  private Integer stepPollInterval;
  private CounterFilter counterFilter;
  private ResourceManager resourceManager;
  private String sandboxDir;
  private TrackerURLBuilder urlBuilder;
  private boolean stopOnFailure = false;
  private HostnameProvider hostnameProvider;

  protected WorkflowOptions() {
  }

  public HostnameProvider getHostnameProvider() {
    return hostnameProvider;
  }

  public WorkflowOptions setHostnameProvider(HostnameProvider hostnameProvider) {
    this.hostnameProvider = hostnameProvider;
    return this;
  }

  public WorkflowOptions setUrlBuilder(TrackerURLBuilder urlBuilder) {
    this.urlBuilder = urlBuilder;
    return this;
  }

  public boolean getStopOnFailure() {
    return stopOnFailure;
  }

  public WorkflowOptions setStopOnFailure(boolean stopOnFailure) {
    this.stopOnFailure = stopOnFailure;
    return this;
  }

  public TrackerURLBuilder getUrlBuilder() {
    return urlBuilder;
  }

  public int getMaxConcurrentSteps() {
    return maxConcurrentSteps;
  }

  public WorkflowOptions setMaxConcurrentSteps(int maxConcurrentSteps) {
    this.maxConcurrentSteps = maxConcurrentSteps;
    return this;
  }

  public WorkflowOptions setStepPollInterval(int ms) {
    this.stepPollInterval = ms;
    return this;
  }

  public Integer getStepPollInterval() {
    return stepPollInterval;
  }

  public AlertsHandler getAlertsHandler() {
    return alertsHandler;
  }

  public WorkflowOptions configureTeam(TeamList team, String subPool){
    addWorkflowProperties(PropertiesUtil.teamPool(team, subPool));
    setAlertsHandler(team);
    return this;
  }

  public WorkflowOptions setAlertsHandler(TeamList team){
    return setAlertsHandler(team, Optional.<Class<?>>absent());
  }

  public WorkflowOptions setAlertsHandler(TeamList team, Class<?> project){
    return setAlertsHandler(team, Optional.<Class<?>>of(project));
  }

  public WorkflowOptions setAlertsHandler(TeamList team, Optional<Class<?>> project){
    return setAlertsHandler(AlertsHandlers.buildHandlerForTeam(team, project));
  }

  public WorkflowOptions setAlertsHandler(AlertsHandler alertsHandler) {
    this.alertsHandler = alertsHandler;
    return this;
  }

  public WorkflowOptions setEnabledNotifications(WorkflowRunnerNotification enabledNotification,
                                                 WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = EnumSet.of(enabledNotification, enabledNotifications);
    return this;
  }

  public WorkflowOptions setEnabledNotifications(Set<WorkflowRunnerNotification> enabledNotifications) {
    this.enabledNotifications = EnumSet.copyOf(enabledNotifications);
    return this;
  }

  public WorkflowOptions setNotificationLevel(Set<WorkflowRunnerNotification> notifications) {
    enabledNotifications = EnumSet.copyOf(notifications);
    return this;
  }

  public WorkflowOptions addWorkflowProperties(Map<Object, Object> propertiesMap) {
    return addWorkflowHadoopProperties(new HadoopProperties(propertiesMap, false));
  }

  public WorkflowOptions addWorkflowHadoopProperties(HadoopProperties newProperties) {
    this.properties = newProperties.override(this.properties);
    return this;
  }

  public WorkflowOptions setResourceManager(ResourceManager resourceManager) {
    this.resourceManager = resourceManager;
    return this;
  }

  public ResourceManager getResourceManager() {
    return this.resourceManager;
  }

  public Set<WorkflowRunnerNotification> getEnabledNotifications() {
    return enabledNotifications;
  }

  public HadoopProperties getWorkflowJobProperties() {
    return properties.override(CascadingHelper.get().getDefaultHadoopProperties());
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

  @Deprecated
  //  all counters are tracked by default now, so this will actually disable tracking most counters.  if you really want this, let me know  -- ben
  public WorkflowOptions setCounterFilter(CounterFilter filter) {
    this.counterFilter = filter;
    return this;
  }

  public String getSandboxDir() {
    return sandboxDir;
  }

  public WorkflowOptions setSandboxDir(String sandboxDir) {
    this.sandboxDir = sandboxDir;
    return this;
  }

}
