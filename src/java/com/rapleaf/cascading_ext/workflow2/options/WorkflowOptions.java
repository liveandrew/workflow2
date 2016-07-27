package com.rapleaf.cascading_ext.workflow2.options;

import java.util.Map;

import com.google.common.collect.Maps;

import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.util.HadoopProperties;
import com.liveramp.cascading_tools.properties.PropertiesUtil;
import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.TrackerURLBuilder;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;

public class WorkflowOptions extends BaseWorkflowOptions<WorkflowOptions> {

  private StoreReaderLockProvider lockProvider;
  private ContextStorage storage;
  private CounterFilter counterFilter;
  private ResourceDeclarer resourceDeclarer;

  private TrackerURLBuilder urlBuilder;
  private HostnameProvider hostnameProvider;

  private OverridableProperties properties = new NestedProperties(Maps.newHashMap(), false);


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

  public TrackerURLBuilder getUrlBuilder() {
    return urlBuilder;
  }


  public WorkflowOptions addWorkflowProperties(Map<Object, Object> propertiesMap) {
    return addWorkflowHadoopProperties(new HadoopProperties(propertiesMap, false));
  }

  public WorkflowOptions addWorkflowHadoopProperties(OverridableProperties newProperties) {
    this.properties = newProperties.override(this.properties);
    return this;
  }

  public WorkflowOptions configureTeam(TeamList team, String subPool) {
    addWorkflowProperties(PropertiesUtil.teamPool(team, subPool));
    setAlertsHandler(team);
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


  public WorkflowOptions setResourceManager(ResourceDeclarer resourceManager) {
    this.resourceDeclarer = resourceManager;
    return this;
  }

  public ResourceDeclarer getResourceManager() {
    return this.resourceDeclarer;
  }


  public OverridableProperties getWorkflowJobProperties() {
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


}
