package com.liveramp.workflow_core;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.LoggingAlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.workflow_core.info.WorkflowInfoConsumer;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.rapleaf.cascading_ext.workflow2.DbTrackerURLBuilder;
import com.rapleaf.cascading_ext.workflow2.TrackerURLBuilder;
import com.rapleaf.cascading_ext.workflow2.WorkflowNotificationLevel;
import com.rapleaf.cascading_ext.workflow2.options.DefaultHostnameProvider;
import com.rapleaf.cascading_ext.workflow2.options.FixedHostnameProvider;
import com.rapleaf.cascading_ext.workflow2.options.HostnameProvider;
import com.rapleaf.cascading_ext.workflow2.rollback.RollbackBehavior;
import com.rapleaf.cascading_ext.workflow2.rollback.SuccessCallback;
import com.rapleaf.support.Rap;

public class BaseWorkflowOptions<T extends BaseWorkflowOptions<T>> {
  public static final String WORKFLOW_UI_URL = "http://workflows.liveramp.net";

  private final OverridableProperties defaultProperties;
  private final Map<Object, Object> systemProperties; //  not for putting in conf, but for visibility into config

  private int maxConcurrentSteps;
  private AlertsHandler alertsHandler;
  private Set<WorkflowRunnerNotification> enabledNotifications;
  private Set<WorkflowRunnerNotification> disabledNotifications = Sets.newHashSet();
  private String uniqueIdentifier;
  private AppType appType;
  private Integer stepPollInterval;
  private String sandboxDir;
  private String description;
  private ContextStorage storage;
  private RollbackBehavior rollBackOnFailure = new RollbackBehavior.Unconditional(false);

  private TrackerURLBuilder urlBuilder;
  private ResourceDeclarer resourceDeclarer;
  private HostnameProvider hostnameProvider;

  private List<SuccessCallback> onSuccess = Lists.newArrayList();

  private OverridableProperties properties = new NestedProperties(Maps.newHashMap(), false);
  private List<WorkflowInfoConsumer> infoConsumerList = Lists.newArrayList();

  protected BaseWorkflowOptions(OverridableProperties defaultProperties) {
    this(defaultProperties, Maps.newHashMap());
  }

  protected BaseWorkflowOptions(OverridableProperties defaultProperties,
                                Map<Object, Object> systemProperties) {
    this.defaultProperties = defaultProperties;
    this.systemProperties = systemProperties;
  }

  public T addSuccessCallback(SuccessCallback callback) {
    onSuccess.add(callback);
    return (T)this;
  }

  public T clearSuccessCallbacks(){
    onSuccess.clear();
    return (T) this;
  }

  public List<SuccessCallback> getSuccessCallbacks() {
    return onSuccess;
  }

  public RollbackBehavior getRollBackBehavior() {
    return rollBackOnFailure;
  }

  public T setRollBackOnFailure(boolean rollBackOnFailure) {
    this.rollBackOnFailure = new RollbackBehavior.Unconditional(rollBackOnFailure);
    return (T)this;
  }

  public T setRollBackBehavior(RollbackBehavior behavior) {
    this.rollBackOnFailure = behavior;
    return (T)this;
  }

  public HostnameProvider getHostnameProvider() {
    return hostnameProvider;
  }

  public T setHostnameProvider(HostnameProvider hostnameProvider) {
    this.hostnameProvider = hostnameProvider;
    return (T)this;
  }

  public T setUrlBuilder(TrackerURLBuilder urlBuilder) {
    this.urlBuilder = urlBuilder;
    return (T)this;
  }

  public TrackerURLBuilder getUrlBuilder() {
    return urlBuilder;
  }


  public String getDescription() {
    return description;
  }

  public T setDescription(String description) {
    this.description = description;
    return (T)this;
  }

  public int getMaxConcurrentSteps() {
    return maxConcurrentSteps;
  }

  public T setMaxConcurrentSteps(int maxConcurrentSteps) {
    this.maxConcurrentSteps = maxConcurrentSteps;
    return (T)this;
  }

  public T setStepPollInterval(int ms) {
    this.stepPollInterval = ms;
    return (T)this;
  }


  public ContextStorage getStorage() {
    return storage;
  }

  public T setStorage(ContextStorage storage) {
    this.storage = storage;
    return (T)this;
  }

  public Integer getStepPollInterval() {
    return stepPollInterval;
  }

  public AlertsHandler getAlertsHandler() {
    return alertsHandler;
  }

  public T setAlertsHandler(TeamList team) {
    return setAlertsHandler(team, Optional.<Class<?>>absent());
  }

  public T setAlertsHandler(TeamList team, Class<?> project) {
    return setAlertsHandler(team, Optional.<Class<?>>of(project));
  }

  public T setAlertsHandler(TeamList team, Optional<Class<?>> project) {
    return setAlertsHandler(AlertsHandlers.buildHandlerForTeam(team, project));
  }

  public T setAlertsHandler(AlertsHandler alertsHandler) {
    this.alertsHandler = alertsHandler;
    return (T)this;
  }

  public T setEnabledNotifications(WorkflowRunnerNotification enabledNotification,
                                   WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = EnumSet.of(enabledNotification, enabledNotifications);
    return (T)this;
  }

  public T setEnabledNotifications(Set<WorkflowRunnerNotification> enabledNotifications) {
    this.enabledNotifications = EnumSet.copyOf(enabledNotifications);
    return (T)this;
  }

  public T setDisabledNotifications(WorkflowRunnerNotification disabledNotification,
                                    WorkflowRunnerNotification... disabledNotifications) {
    this.disabledNotifications = EnumSet.of(disabledNotification, disabledNotifications);
    return (T)this;
  }

  public T setNotificationLevel(Set<WorkflowRunnerNotification> notifications) {
    enabledNotifications = EnumSet.copyOf(notifications);
    return (T)this;
  }

  public Set<WorkflowRunnerNotification> getEnabledNotifications() {
    return Sets.difference(enabledNotifications, disabledNotifications);
  }

  public String getScopeIdentifier() {
    return uniqueIdentifier;
  }

  public T setUniqueIdentifier(String uniqueIdentifier) {
    if (uniqueIdentifier != null && uniqueIdentifier.equals("__NULL")) {
      throw new IllegalArgumentException("This is temporarily a reserved scope, while making scope not null");
    }
    this.uniqueIdentifier = uniqueIdentifier;
    return (T)this;
  }

  public AppType getAppType() {
    return appType;
  }

  public T setAppType(AppType appType) {
    this.appType = appType;
    return (T)this;
  }

  public String getSandboxDir() {
    return sandboxDir;
  }

  public T setSandboxDir(String sandboxDir) {
    this.sandboxDir = sandboxDir;
    return (T)this;
  }

  public T setResourceManager(ResourceDeclarer resourceManager) {
    this.resourceDeclarer = resourceManager;
    return (T)this;
  }

  public ResourceDeclarer getResourceManager() {
    return this.resourceDeclarer;
  }


  public T addWorkflowProperties(Map<Object, Object> propertiesMap) {
    return addWorkflowHadoopProperties(new NestedProperties(propertiesMap, false));
  }

  public T addWorkflowHadoopProperties(OverridableProperties newProperties) {
    this.properties = newProperties.override(this.properties);
    return (T)this;
  }

  public OverridableProperties getWorkflowJobProperties() {
    return properties.override(defaultProperties);
  }


  public Object getConfiguredProperty(String property) {

    //  first look at configured properties
    Map<Object, OverridableProperties.Property> map = properties.getMap();
    if (map.containsKey(property)) {
      return map.get(property).value;
    }

    //  then stuff in the jobconf
    Object value = systemProperties.get(property);
    if (value != null) {
      return value;
    }

    return null;
  }

  public T addWorkflowInfoConsumer(WorkflowInfoConsumer consumer) {
    this.infoConsumerList.add(consumer);
    return (T)this;
  }

  public List<WorkflowInfoConsumer> getWorkflowInfoConsumers() {
    return this.infoConsumerList;
  }

  //  static helpers

  public static BaseWorkflowOptions production() {
    BaseWorkflowOptions opts = new BaseWorkflowOptions(new NestedProperties(Maps.newHashMap(), false));
    configureProduction(opts);
    return opts;
  }

  protected static void configureProduction(BaseWorkflowOptions opts) {
    Rap.assertProduction();
    opts
        .setMaxConcurrentSteps(Integer.MAX_VALUE)
        .setAlertsHandler(new LoggingAlertsHandler())
        .setNotificationLevel(WorkflowNotificationLevel.ERROR)
        .setStorage(new ContextStorage.None())
        .setStepPollInterval(6000)  // be nice to production DB
        .setUrlBuilder(new DbTrackerURLBuilder(WORKFLOW_UI_URL))
        .setHostnameProvider(new DefaultHostnameProvider())
        .setResourceManager(new ResourceManager.NotImplemented())
        .addSuccessCallback(DataDogDurationPusher.production());
  }

  public static BaseWorkflowOptions test() {
    Rap.assertTest();

    BaseWorkflowOptions opts = new BaseWorkflowOptions(new NestedProperties(Maps.newHashMap(), false));
    configureTest(opts);
    return opts;
  }


  protected static void configureTest(BaseWorkflowOptions opts) {
    opts.setMaxConcurrentSteps(1)
        .setAlertsHandler(new LoggingAlertsHandler())
        .setNotificationLevel(WorkflowNotificationLevel.DEBUG)
        .setStorage(new ContextStorage.None())
        .setStepPollInterval(100)
        .setUrlBuilder(new TrackerURLBuilder.None())
        .setHostnameProvider(new FixedHostnameProvider())
        .setResourceManager(new ResourceDeclarer.NotImplemented());
  }

}
