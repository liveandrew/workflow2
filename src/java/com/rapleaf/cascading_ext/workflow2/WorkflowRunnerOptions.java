package com.rapleaf.cascading_ext.workflow2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;

public class WorkflowRunnerOptions {

  private int maxConcurrentSteps;
  private List<String> notificationRecipients;
  private Map<Object, Object> workflowJobProperties = Maps.newHashMap();
  private WorkflowRunnerNotificationSet enabledNotifications;
  private int statsDPort;
  private String statsDHost;
  private StoreReaderLockProvider lockProvider;
  private ContextStorage storage;
  private WorkflowRegistry registry;

  public WorkflowRunnerOptions() {
    maxConcurrentSteps = Integer.MAX_VALUE;
    notificationRecipients = null;
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

  public List<String> getNotificationRecipients() {
    return notificationRecipients;
  }

  public WorkflowRunnerOptions setNotificationRecipients(String... notificationRecipients) {
    this.notificationRecipients = Arrays.asList(notificationRecipients);
    return this;
  }

  public WorkflowRegistry getRegistry() {
    return registry;
  }

  public void setRegistry(WorkflowRegistry registry) {
    this.registry = registry;
  }

  public WorkflowRunnerOptions setNotificationRecipients(List<String> notificationEmails) {
    this.notificationRecipients = notificationEmails;
    return this;
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

  public ContextStorage getStorage() {
    return storage;
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
