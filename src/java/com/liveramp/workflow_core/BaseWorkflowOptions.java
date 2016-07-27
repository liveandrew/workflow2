package com.liveramp.workflow_core;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.rapleaf.cascading_ext.workflow2.TrackerURLBuilder;
import com.rapleaf.cascading_ext.workflow2.options.HostnameProvider;

public class BaseWorkflowOptions<T extends BaseWorkflowOptions<T>> {

  private int maxConcurrentSteps;
  private AlertsHandler alertsHandler;
  private Set<WorkflowRunnerNotification> enabledNotifications;
  private Set<WorkflowRunnerNotification> disabledNotifications = Sets.newHashSet();
  private String uniqueIdentifier;
  private AppType appType;
  private Integer stepPollInterval;
  private String sandboxDir;
  private String description;

  private TrackerURLBuilder urlBuilder;

  private HostnameProvider hostnameProvider;


  public HostnameProvider getHostnameProvider() {
    return hostnameProvider;
  }

  public T setHostnameProvider(HostnameProvider hostnameProvider) {
    this.hostnameProvider = hostnameProvider;
    return (T) this;
  }

  public T setUrlBuilder(TrackerURLBuilder urlBuilder) {
    this.urlBuilder = urlBuilder;
    return (T) this;
  }

  public TrackerURLBuilder getUrlBuilder() {
    return urlBuilder;
  }


  public String getDescription() {
    return description;
  }

  public T setDescription(String description) {
    this.description = description;
    return (T) this;
  }

  public int getMaxConcurrentSteps() {
    return maxConcurrentSteps;
  }

  public T setMaxConcurrentSteps(int maxConcurrentSteps) {
    this.maxConcurrentSteps = maxConcurrentSteps;
    return (T) this;
  }

  public T setStepPollInterval(int ms) {
    this.stepPollInterval = ms;
    return (T) this;
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
    return (T) this;
  }

  public T setEnabledNotifications(WorkflowRunnerNotification enabledNotification,
                                                 WorkflowRunnerNotification... enabledNotifications) {
    this.enabledNotifications = EnumSet.of(enabledNotification, enabledNotifications);
    return (T) this;
  }

  public T setEnabledNotifications(Set<WorkflowRunnerNotification> enabledNotifications) {
    this.enabledNotifications = EnumSet.copyOf(enabledNotifications);
    return (T) this;
  }

  public T setDisabledNotifications(WorkflowRunnerNotification disabledNotification,
                                                  WorkflowRunnerNotification... disabledNotifications) {
    this.disabledNotifications = EnumSet.of(disabledNotification, disabledNotifications);
    return (T) this;
  }

  public T setNotificationLevel(Set<WorkflowRunnerNotification> notifications) {
    enabledNotifications = EnumSet.copyOf(notifications);
    return (T) this;
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
    return (T) this;
  }

  public AppType getAppType() {
    return appType;
  }

  public T setAppType(AppType appType) {
    this.appType = appType;
    return (T) this;
  }

  public String getSandboxDir() {
    return sandboxDir;
  }

  public T setSandboxDir(String sandboxDir) {
    this.sandboxDir = sandboxDir;
    return (T) this;
  }

}
