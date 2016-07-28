package com.liveramp.workflow_core.runner;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.commons.collections.properties.OverridableProperties;
import com.liveramp.java_support.workflow.ActionId;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.DataStoreInfo;
import com.liveramp.workflow_state.StepState;
import com.liveramp.workflow_state.WorkflowStatePersistence;

public abstract class BaseAction<Config> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAction.class);

  private final ActionId actionId;

  private OverridableProperties stepProperties;
  private OverridableProperties combinedProperties;

  //  TODO this doesn't really belong here
  private boolean failOnCounterFetch = true;

  private transient WorkflowStatePersistence persistence;
  private Config config;

  public BaseAction(String checkpointToken) {
    this(checkpointToken, Maps.newHashMap());
  }

  public BaseAction(String checkpointToken, Map<Object, Object> properties){
    this.actionId = new ActionId(checkpointToken);
    this.stepProperties = new NestedProperties(properties, false);
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

  protected WorkflowStatePersistence getPersistence(){
    return persistence;
  }

  protected OverridableProperties getCombinedProperties(){
    return combinedProperties;
  }

  protected OverridableProperties getStepProperties(){
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

  //  after execute, either fail or succeed
  protected void postExecute() {
    //  default no op
  }

  //  inputs and outputs of the action
  public Multimap<DSAction, DataStoreInfo> getAllDataStoreInfo(){
    return HashMultimap.create(); // TODO make Action stop storing DataStore and store DataStoreInfo in this class
  }

    //  not really public : / make package private after cleanup
  public final void setOptionObjects(WorkflowStatePersistence persistence,
                                     Config context){
    this.persistence = persistence;
    this.config = context;
    initialize(context);
  }

  //  not really public : /
  public final void internalExecute(OverridableProperties parentProperties) {

    try {

      combinedProperties = stepProperties.override(parentProperties);

      preExecute();

      execute();

    } catch (Throwable t) {
      LOG.error("Action " + fullId() + " failed due to Throwable", t);
      throw wrapRuntimeException(t);
    } finally {
      postExecute();
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
      LOG.warn("Couldn't set status message.");
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
  public TwoNestedMap<String, String, Long> getStepCounters() throws IOException {
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
