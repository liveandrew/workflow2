package com.liveramp.workflow_core.runner;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.ReadResourceContainer;
import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.WriteResource;
import com.liveramp.cascading_ext.resource.WriteResourceContainer;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.liveramp.java_support.workflow.ActionId;
import com.liveramp.workflow_core.ContextStorage;
import com.liveramp.workflow_core.OldResource;
import com.liveramp.workflow_core.ResourceFactory;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.DataStoreInfo;
import com.liveramp.workflow_state.StepState;
import com.liveramp.workflow_state.WorkflowStatePersistence;

public abstract class BaseAction<Config> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAction.class);

  private final ActionId actionId;

  private OverridableProperties stepProperties;
  private OverridableProperties combinedProperties;

  private final Map<Resource, ReadResourceContainer> readResources = Maps.newHashMap();
  private final Map<Resource, WriteResourceContainer> writeResources = Maps.newHashMap();
  private final ResourceFactory resourceFactory;

  private final Multimap<ResourceAction, OldResource> resources = HashMultimap.create();

  private ContextStorage storage;
  private ResourceManager resourceManager;

  //  TODO this doesn't really belong here
  private boolean failOnCounterFetch = true;

  private transient WorkflowStatePersistence persistence;
  private Config config;

  public BaseAction(String checkpointToken) {
    this(checkpointToken, Maps.newHashMap());
  }

  public BaseAction(String checkpointToken, Map<Object, Object> properties) {
    this.actionId = new ActionId(checkpointToken);
    this.stepProperties = new NestedProperties(properties, false);
    this.resourceFactory = new ResourceFactory(getActionId());
  }

  //  it's tempting to reuse DSAction for this, but I think some DSActions have no parallel for in-memory resources
  //  which actually make sense...
  public static enum ResourceAction {
    CREATES,
    USES
  }

  public void setFailOnCounterFetch(boolean value) {
    this.failOnCounterFetch = value;
  }

  public ActionId getActionId() {
    return actionId;
  }

  public String fullId() {
    return actionId.resolve();
  }

  protected WorkflowStatePersistence getPersistence() {
    return persistence;
  }

  protected OverridableProperties getCombinedProperties() {
    return combinedProperties;
  }

  protected OverridableProperties getStepProperties() {
    return stepProperties;
  }

  protected Config getConfig() {
    return config;
  }

  protected boolean isFailOnCounterFetch() {
    return failOnCounterFetch;
  }

  //  before workflow runs
  protected void initialize(Config context) {
    //  default no op
  }

  //  when step runs, before Action execute
  protected void preExecute() throws Exception {
    //  default no op
  }

  //  action work, implemented by end-user
  protected abstract void execute() throws Exception;

  protected void rollback() throws Exception {

  }

  //  after execute, either fail or succeed
  protected void postExecute() {
    //  default no op
  }

  //  inputs and outputs of the action
  public Multimap<DSAction, DataStoreInfo> getAllDataStoreInfo() {
    return HashMultimap.create(); // TODO make Action stop storing DataStore and store DataStoreInfo in this class
  }

  //  not really public : / make package private after cleanup
  public final void setOptionObjects(WorkflowStatePersistence persistence,
                                     ResourceManager resourceManager,
                                     ContextStorage storage,
                                     Config context) {
    this.persistence = persistence;
    this.config = context;

    this.resourceManager = resourceManager;
    this.storage = storage;

    initialize(context);
  }


  //  resource actions

  @Deprecated
  protected void creates(OldResource resource) {
    mark(ResourceAction.CREATES, resource);
  }

  @Deprecated
  protected void uses(OldResource resource) {
    mark(ResourceAction.USES, resource);
  }

  @Deprecated
  private void mark(ResourceAction action, OldResource resource) {
    resources.put(action, resource);
  }

  protected <T> ReadResource<T> readsFrom(Resource<T> resource) {
    ReadResourceContainer<T> container = new ReadResourceContainer<T>();
    readResources.put(resource, container);
    return container;
  }

  protected <T> WriteResource<T> creates(Resource<T> resource) {
    WriteResourceContainer<T> container = new WriteResourceContainer<T>();
    writeResources.put(resource, container);
    return container;
  }


  @Deprecated
  protected <T> T get(OldResource<T> resource) throws IOException {
    if (!resources.get(ResourceAction.USES).contains(resource)) {
      throw new RuntimeException("Cannot use resource without declaring it with uses()");
    }

    return storage.get(resource);
  }

  protected <T> T get(ReadResource<T> resource) {
    return (T)resourceManager.read(resource);
  }

  protected <T, R extends WriteResource<T>> void set(R resource, T value) {
    resourceManager.write(resource, value);
  }

  @Deprecated
  protected <T> void set(OldResource<T> resource, T value) throws IOException {
    if (!resources.get(ResourceAction.CREATES).contains(resource)) {
      throw new RuntimeException("Cannot set resource without declaring it with creates()");
    }

    storage.set(resource, value);
  }


  protected <T> OldResource<T> resource(String name) {
    return resourceFactory().makeResource(name);
  }

  public ResourceFactory resourceFactory() {
    return resourceFactory;
  }


  //  not really public : /
  public final void internalExecute(OverridableProperties parentProperties) {

    try {

      combinedProperties = stepProperties.override(parentProperties);

      preExecute();

      LOG.info("Setting read resources");
      for (Resource resource : readResources.keySet()) {
        readResources.get(resource).setResource(resourceManager.getReadPermission(resource));
      }

      LOG.info("Setting write resources");
      for (Resource resource : writeResources.keySet()) {
        writeResources.get(resource).setResource(resourceManager.getWritePermission(resource));
      }

      execute();

    } catch (Throwable t) {
      LOG.error("Action " + fullId() + " failed due to Throwable", t);
      throw wrapRuntimeException(t);
    } finally {
      postExecute();
    }
  }

  protected final void internalRollback(OverridableProperties properties){

    try {

      combinedProperties = stepProperties.override(properties);

      rollback();

    } catch (Throwable t) {
      LOG.error("Rollback of action " + fullId() + " failed due to Throwable", t);
      throw wrapRuntimeException(t);
    }
  }

  private static RuntimeException wrapRuntimeException(Throwable t) {
    return (t instanceof RuntimeException) ? (RuntimeException)t : new RuntimeException(t);
  }

  /**
   * Set an application-specific status message to display. This will be
   * visible in the logs as well as through the UI. This is a great place to
   * report progress or choices.
   *
   * @param statusMessage
   */
  protected void setStatusMessage(String statusMessage) throws IOException {
    LOG.info("Status Message: " + statusMessage);
    persistence.markStepStatusMessage(fullId(), statusMessage);
  }

  /**
   * Same as {@link #setStatusMessage(String)} but only logs failures
   * doesn't rethrow.
   */
  protected void setStatusMessageSafe(String message) {
    try {
      setStatusMessage(message);
    } catch (Exception e) {
      LOG.warn("Couldn't set status message.", e);
    }
  }

  protected long getCurrentExecutionId() throws IOException {
    return persistence.getExecutionId();
  }

  protected TwoNestedMap<String, String, Long> getFlatCounters() throws IOException {
    return persistence.getFlatCounters();
  }

  protected ThreeNestedMap<String, String, String, Long> getAllCountersByStep() throws IOException {
    return persistence.getCountersByStep();
  }

  //  TODO package-private after class cleanup
  public TwoNestedMap<String, String, Long> getCurrentStepCounters() throws IOException {
    ThreeNestedMap<String, String, String, Long> allCounters = persistence.getCountersByStep();
    return allCounters.get(getActionId().resolve());
  }

  //  TODO package-private after class cleanup
  public DurationInfo getDurationInfo() throws IOException {
    Map<String, StepState> stepStatuses = persistence.getStepStates();
    StepState stepState = stepStatuses.get(getActionId().resolve());
    return new DurationInfo(stepState.getStartTimestamp(), stepState.getEndTimestamp());
  }

  public static class DurationInfo {
    private final long startTime;
    private final long endTime;

    public DurationInfo(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getEndTime() {
      return endTime;
    }
  }


  @Override
  public String toString() {
    return getClass().getSimpleName() + " [checkpointToken=" + getActionId().getRelativeName() + "]";
  }

}
