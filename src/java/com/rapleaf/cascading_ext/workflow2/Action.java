package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.scribe.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.flow.JobPersister;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.ReadResourceContainer;
import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.WriteResource;
import com.liveramp.cascading_ext.resource.WriteResourceContainer;
import com.liveramp.cascading_ext.util.HadoopProperties;
import com.liveramp.cascading_tools.jobs.TrackedFlow;
import com.liveramp.cascading_tools.jobs.TrackedOperation;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.java_support.workflow.ActionId;
import com.liveramp.java_support.workflow.TaskSummary;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.StepState;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;
import com.rapleaf.cascading_ext.workflow2.flow_closure.FlowRunner;

public abstract class Action {
  private static final Logger LOG = LoggerFactory.getLogger(Action.class);

  private final ActionId actionId;
  private final String tmpRoot;

  private final Multimap<DSAction, DataStore> datastores = HashMultimap.create();
  private final Map<Resource, ReadResourceContainer> readResources = Maps.newHashMap();
  private final Map<Resource, WriteResourceContainer> writeResources = Maps.newHashMap();
  private final ResourceFactory resourceFactory;
  private final DataStoreBuilder builder;

  private ResourceManager resourceManager;


  //  it's tempting to reuse DSAction for this, but I think some DSActions have no parallel for in-memory resources
  //  which actually make sense...
  public static enum ResourceAction {
    CREATES,
    USES
  }

  private final Multimap<ResourceAction, OldResource> resources = HashMultimap.create();

  private StoreReaderLockProvider lockProvider;
  private StoreReaderLockProvider.LockManager lockManager;
  private HadoopProperties stepProperties;
  private HadoopProperties combinedProperties;

  private FileSystem fs;

  private transient ContextStorage storage;
  private transient WorkflowStatePersistence persistence;
  private transient CounterFilter counterFilter;

  private boolean failOnCounterFetch = true;

  public Action(String checkpointToken) {
    this(checkpointToken, Maps.newHashMap());
  }

  public Action(String checkpointToken, String tmpRoot) {
    this(checkpointToken, tmpRoot, Maps.newHashMap());
  }

  public Action(String checkpointToken, Map<Object, Object> properties) {
    this(checkpointToken, null, properties);
  }

  public Action(String checkpointToken, String tmpRoot, Map<Object, Object> properties) {
    this.actionId = new ActionId(checkpointToken);
    this.stepProperties = new HadoopProperties(properties, false);
    this.resourceFactory = new ResourceFactory(actionId);

    if (tmpRoot != null) {
      this.tmpRoot = tmpRoot + "/" + checkpointToken + "-tmp-stores";
      this.builder = new DataStoreBuilder(getTmpRoot());
    } else {
      this.tmpRoot = null;
      this.builder = null;
    }

  }

  public ActionId getActionId() {
    return actionId;
  }

  protected FileSystem getFS() throws IOException {
    if (fs == null) {
      fs = FileSystemHelper.getFS();
    }

    return fs;
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


  //  datastore actions

  protected void readsFrom(DataStore store) {
    mark(DSAction.READS_FROM, store);
  }

  protected void creates(DataStore store) {
    mark(DSAction.CREATES, store);
  }

  protected void createsTemporary(DataStore store) {
    mark(DSAction.CREATES_TEMPORARY, store);
  }

  protected void writesTo(DataStore store) {
    mark(DSAction.WRITES_TO, store);
  }

  protected void consumes(DataStore store) {
    mark(DSAction.CONSUMES, store);
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

  private void mark(DSAction action, DataStore store) {
    Preconditions.checkNotNull(store, "Cannot mark a null datastore as used!");
    datastores.put(action, store);
  }

  // Don't call this method directly!
  protected abstract void execute() throws Exception;

  public DataStoreBuilder builder() {
    return builder;
  }

  protected <T> OldResource<T> resource(String name) {
    return resourceFactory().makeResource(name);
  }

  public ResourceFactory resourceFactory() {
    return resourceFactory;
  }


  @Deprecated
  protected <T> T get(OldResource<T> resource) throws IOException {
    if (!resources.get(ResourceAction.USES).contains(resource)) {
      throw new RuntimeException("Cannot use resource without declaring it with uses()");
    }

    return storage.get(resource);
  }

  protected <T> T get(ReadResource<T> resource) {
    if (resourceManager == null) {
      throw new RuntimeException("Cannot call get() without providing a ResourceManager to the WorkflowRunner.");
    }
    return (T)resourceManager.read(resource);
  }

  protected <T, R extends WriteResource<T>> void set(R resource, T value) {
    if (resourceManager == null) {
      throw new RuntimeException("Cannot call set() without providing a ResourceManager to the WorkflowRunner.");
    }
    resourceManager.write(resource, value);
  }

  @Deprecated
  protected <T> void set(OldResource<T> resource, T value) throws IOException {
    if (!resources.get(ResourceAction.CREATES).contains(resource)) {
      throw new RuntimeException("Cannot set resource without declaring it with creates()");
    }

    storage.set(resource, value);
  }

  public String fullId() {
    return actionId.resolve();
  }

  protected final void internalExecute(HadoopProperties parentProperties) {

    try {

      //  only set properties not explicitly set by the step
      combinedProperties = stepProperties.override(parentProperties);

      prepDirs();

      lockManager.lockConsumeStart();

      if (!(readResources.isEmpty() && writeResources.isEmpty()) && resourceManager == null) {
        throw new RuntimeException("Cannot call readsFrom() or creates() on resources without setting a ResourceManager in the WorkflowOptions!");
      }

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

      lockManager.release();

    }
  }

  private static RuntimeException wrapRuntimeException(Throwable t) {
    return (t instanceof RuntimeException) ? (RuntimeException)t : new RuntimeException(t);
  }

  public Set<DataStore> getDatastores(DSAction... actions) {
    Set<DataStore> stores = Sets.newHashSet();

    for (DSAction dsAction : actions) {
      stores.addAll(datastores.get(dsAction));
    }

    return stores;
  }

  public Multimap<DSAction, DataStore> getAllDatastores() {
    return datastores;
  }

  @SuppressWarnings("PMD.BlacklistedMethods") //  temporary hopefully, until we get more cluster space
  private void prepDirs() throws Exception {
    FileSystem fs = FileSystemHelper.getFS();
    for (DataStore ds : getDatastores(DSAction.CREATES, DSAction.CREATES_TEMPORARY)) {
      String uri = new URI(ds.getPath()).getPath();
      Path path = new Path(ds.getPath());
      Boolean trashEnabled = TrashHelper.isEnabled();

      if (fs.exists(path)) {
        // delete if tmp store, or if no trash is enabled
        if (uri.startsWith("/tmp/") || !trashEnabled) {
          LOG.info("Deleting " + uri);
          fs.delete(path, true);
          // otherwise, move to trash
        } else {
          LOG.info("Moving to trash: " + uri);
          TrashHelper.moveToTrash(fs, path);
        }
      }
    }
  }

  public final String getTmpRoot() {
    if (tmpRoot == null) {
      throw new RuntimeException("Temp root not set for action " + this.toString());
    }
    return tmpRoot;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [checkpointToken=" + getActionId().getRelativeName() + "]";
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

  protected void setOptionObjects(StoreReaderLockProvider lockProvider,
                                  WorkflowStatePersistence persistence,
                                  ContextStorage storage,
                                  CounterFilter counterFilter,
                                  ResourceManager resourceManager) {
    this.lockProvider = lockProvider;
    this.persistence = persistence;
    this.storage = storage;
    this.counterFilter = counterFilter;
    this.resourceManager = resourceManager;
    this.lockManager = lockProvider
        .createManager(getDatastores(DSAction.READS_FROM))
        .lockProcessStart();
  }

  protected StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }

  protected FlowBuilder buildFlow(Map<Object, Object> properties) {
    return new FlowBuilder(buildFlowConnector(getInheritedProperties(properties)), getClass());
  }

  protected Map<Object, Object> getInheritedProperties() {
    return getInheritedProperties(Maps.newHashMap());
  }

  protected Map<Object, Object> getInheritedProperties(Map<Object, Object> childProperties) {

    HadoopProperties childProps = new HadoopProperties(childProperties, false);

    if (combinedProperties != null) {
      return childProps.override(combinedProperties).getPropertiesMap();
    }
    //TODO Sweep direct calls to execute() so we don't have to do this!
    else {
      return childProps.override(stepProperties.override(CascadingHelper.get().getDefaultHadoopProperties()))
          .getPropertiesMap();
    }
  }

  protected FlowBuilder buildFlow() {
    return buildFlow(Maps.newHashMap());
  }

  protected FlowConnector buildFlowConnector() {
    return CascadingHelper.get().getFlowConnector(combinedProperties.getPropertiesMap());
  }

  private FlowConnector buildFlowConnector(Map<Object, Object> properties) {
    return CascadingUtil.buildFlowConnector(
        new JobConf(),
        properties,
        CascadingHelper.get().resolveFlowStepStrategies(),
        CascadingHelper.get().getInvalidPropertyValues()
    );
  }

  protected Flow completeWithProgress(FlowBuilder.IFlowClosure flowc) {
    return completeWithProgress(flowc, false);
  }

  //  TODO sweep when we figure out cascading npe (prolly upgrade past 2.5.1)
  protected Flow completeWithProgress(FlowBuilder.IFlowClosure flowc, boolean skipCompleteListener) {
    Flow flow = flowc.buildFlow();

    TrackedOperation tracked = new TrackedFlow(flow, skipCompleteListener);
    completeWithProgress(tracked);

    return flow;
  }


  protected JobPersister getPersister(){
    return new WorkflowJobPersister(persistence, getActionId().resolve(), counterFilter);
  }

  protected void completeWithProgress(TrackedOperation tracked){
    JobPersister persister = getPersister();
    tracked.complete(
        persister,
        failOnCounterFetch
    );
  }

  protected FlowRunner completeWithProgressClosure() {
    return new ActionFlowRunner();
  }

  private class ActionFlowRunner implements FlowRunner {
    @Override
    public Flow complete(Properties properties, String name, Tap source, Tap sink, Pipe tail) {
      return completeWithProgress(buildFlow(properties).connect(name, source, sink, tail));
    }
  }

  public void setFailOnCounterFetch(boolean value) {
    this.failOnCounterFetch = value;
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

  TwoNestedMap<String, String, Long> getStepCounters() throws IOException {
    String actionId = this.actionId.resolve();
    ThreeNestedMap<String, String, String, Long> allCounters = persistence.getCountersByStep();
    return allCounters.get(actionId);
  }

  DurationInfo getDurationInfo() throws IOException {
    String actionId = this.actionId.resolve();
    Map<String, StepState> stepStatuses = persistence.getStepStatuses();
    StepState stepState = stepStatuses.get(actionId);
    return new DurationInfo(stepState.getStartTimestamp(), stepState.getEndTimestamp());
  }

  //  TODO I don't love having a new class here but returning just a StepState doesn't make sense with
  // calling futures on MSAs and requiring people to specify checkpoint tokens to fetch data breaks modularity
  //  (shouldn't have to know inner tokens of other actions you're using)
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

  //  everything we feel like exposing to pre-execute hooks in CA2.  I don't really love that it's here, but this way
  //  we don't have to make these methods public.  there should be a cleaner way but I can't think of it.
  public class PreExecuteContext {

    public <T> T get(OldResource<T> resource) throws IOException {
      return Action.this.get(resource);
    }

    public <T> T get(ReadResource<T> resource) {
      return Action.this.get(resource);
    }

  }

  //  stuff available for during action construction
  public class ConstructContext {

    public void creates(DataStore store) {
      Action.this.creates(store);
    }

    public <T> void uses(OldResource<T> resource) {
      Action.this.uses(resource);
    }

    public <T> ReadResource<T> readsFrom(Resource<T> resource) {
      return Action.this.readsFrom(resource);
    }

  }

  public PreExecuteContext getPreExecuteContext() {
    return new PreExecuteContext();
  }

  public ConstructContext getConstructContext() {
    return new ConstructContext();
  }

}
