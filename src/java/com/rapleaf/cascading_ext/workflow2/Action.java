package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
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
import cascading.flow.FlowStepStrategy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.flow.LoggingFlowConnector;
import com.liveramp.cascading_ext.flow_step_strategy.FlowStepStrategyFactory;
import com.liveramp.cascading_ext.flow_step_strategy.MultiFlowStepStrategy;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.ReadResourceContainer;
import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.WriteResource;
import com.liveramp.cascading_ext.resource.WriteResourceContainer;
import com.liveramp.cascading_ext.util.HadoopProperties;
import com.liveramp.cascading_ext.util.NestedProperties;
import com.liveramp.cascading_ext.util.OperationStatsUtils;
import com.liveramp.cascading_tools.jobs.ActionOperation;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.java_support.workflow.ActionId;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.RunnableJob;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.liveramp.cascading_tools.jobs.FlowOperation;
import com.liveramp.cascading_tools.jobs.HadoopOperation;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;
import com.rapleaf.cascading_ext.workflow2.flow_closure.FlowRunner;
import com.rapleaf.db_schemas.rldb.workflow.DSAction;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowStatePersistence;

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
  private NestedProperties nestedProperties;

  private List<ActionOperation> operations = new ArrayList<ActionOperation>();

  private FileSystem fs;

  private JobPoller jobPoller = null;

  private transient ContextStorage storage;
  private transient WorkflowStatePersistence persistence;
  private transient CounterFilter counterFilter;

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

  public void runningFlow(ActionOperation operation) {
    operations.add(operation);
  }

  public void runningFlow(Flow flow) {
    operations.add(new FlowOperation(flow));
  }

  public List<ActionOperation> getRunFlows() {
    return operations;
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

  protected final void internalExecute(NestedProperties parentProperties) {

    try {

      //  only set properties not explicitly set by the step
      nestedProperties = new NestedProperties(parentProperties, stepProperties);

      jobPoller = new JobPoller(fullId(), operations, persistence);
      jobPoller.start();

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

      jobPoller.shutdown();

    } catch (Throwable t) {
      LOG.error("Action " + fullId() + " failed due to Throwable", t);
      throw wrapRuntimeException(t);
    } finally {

      lockManager.release();

      if (jobPoller != null) {

        //  make sure each MR job exists in the DB
        //  try to refresh info in case of fast failures
        jobPoller.updateRunningJobs();

        for (ActionOperation operation : operations) {
          recordCounters(operation);
        }

        jobPoller.shutdown();
      }
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
    NestedProperties flowProperties;
    //TODO Sweep direct calls to execute() so we don't have to do this!
    if (nestedProperties != null) {
      flowProperties = new NestedProperties(nestedProperties, properties);
    } else {
      flowProperties =
          new NestedProperties(
              new NestedProperties(
                  new NestedProperties(
                      null,
                      CascadingHelper.get().getDefaultHadoopProperties()
                  ),
                  stepProperties
              ),
              properties
          );
    }
    return new FlowBuilder(buildFlowConnector(flowProperties.getPropertiesMap()), getClass());
  }

  protected FlowBuilder buildFlow() {
    return buildFlow(Maps.newHashMap());
  }

  protected FlowConnector buildFlowConnector() {
    return CascadingHelper.get().getFlowConnector(nestedProperties.getPropertiesMap());
  }

  private FlowConnector buildFlowConnector(Map<Object, Object> properties) {
    List<FlowStepStrategy<JobConf>> strategies = Lists.newArrayList();
    for (FlowStepStrategyFactory<JobConf> factory : CascadingHelper.get().getDefaultFlowStepStrategies()) {
      strategies.add(factory.getFlowStepStrategy());
    }
    return new LoggingFlowConnector(properties,
        new MultiFlowStepStrategy(strategies),
        OperationStatsUtils.formatStackPosition(OperationStatsUtils.getStackPosition(2)));
  }

  protected void completeWithProgress(RunnableJob job) {
    job.addProperties(nestedProperties.getPropertiesMap());
    completeWithProgress(new HadoopOperation(job));
  }

  protected Flow completeWithProgress(FlowBuilder.IFlowClosure flowc) {
    Flow flow = flowc.buildFlow();
    completeWithProgress(new FlowOperation(flow));
    return flow;
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

  /**
   * Complete the provided ActionOperation while monitoring and reporting its progress.
   * This method will call setPercentComplete with values between startPct and
   * maxPct incrementally based on the completion of the ActionOperation's steps.
   *
   * @param operation
   */
  private void completeWithProgress(ActionOperation operation) {
    runningFlow(operation);
    operation.complete();
  }

  protected ActionOperation.Complete completeCallback(){
    return new ActionOperation.Complete() {
      @Override
      public void complete(ActionOperation operation) {
        completeWithProgress(operation);
      }
    };
  }

  private void recordCounters(ActionOperation operation) {

    ThreeNestedMap<String, String, String, Long> counters = operation.getJobCounters();

    for (String job : counters.key1Set()) {
      TwoNestedMap<String, String, Long> toRecord = new TwoNestedMap<String, String, Long>();
      for (String group : counters.key2Set(job)) {
        for (String name : counters.key3Set(job, group)) {
          if (counterFilter.isRecord(group, name)) {
            toRecord.put(group, name, counters.get(job, group, name));
          }
        }
      }
      try {
        persistence.markJobCounters(fullId(), job, toRecord);
      } catch (IOException e) {
        LOG.error("Failed to capture stats for job: " + job);
      }
    }

  }

  protected long getCurrentExecutionId() throws IOException {
    return persistence.getExecutionId();
  }

}
