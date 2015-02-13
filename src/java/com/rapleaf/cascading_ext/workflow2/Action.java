package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.cascading_ext.megadesk.ResourceSemaphore;
import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.java_support.workflow.ActionId;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.workflow2.action_operations.FlowOperation;
import com.rapleaf.cascading_ext.workflow2.action_operations.HadoopOperation;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;
import com.rapleaf.db_schemas.rldb.workflow.DSAction;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowStatePersistence;

public abstract class Action {
  private static final Logger LOG = Logger.getLogger(Action.class);

  private final ActionId actionId;
  private final String tmpRoot;

  private final Multimap<DSAction, DataStore> datastores = HashMultimap.create();
  private final ResourceFactory resourceFactory;
  private final DataStoreBuilder builder;

  //  it's tempting to reuse DSAction for this, but I think some DSActions have no parallel for in-memory resources
  //  which actually make sense...
  public static enum ResourceAction {
    CREATES,
    USES
  }

  private final Multimap<ResourceAction, Resource> resources = HashMultimap.create();

  private StoreReaderLockProvider lockProvider;
  private Map<Object, Object> stepProperties;

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
    this.stepProperties = properties;
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

  protected void creates(Resource resource) {
    mark(ResourceAction.CREATES, resource);
  }

  protected void uses(Resource resource) {
    mark(ResourceAction.USES, resource);
  }

  private void mark(ResourceAction action, Resource resource) {
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

  private void mark(DSAction action, DataStore store) {
    datastores.put(action, store);
  }

  protected abstract void execute() throws Exception;

  public DataStoreBuilder builder() {
    return builder;
  }

  protected <T> Resource<T> resource(String name) {
    return resourceFactory().makeResource(name);
  }

  public ResourceFactory resourceFactory() {
    return resourceFactory;
  }


  protected <T> T get(Resource<T> resource) throws IOException, ClassNotFoundException {
    if (!resources.get(ResourceAction.USES).contains(resource)) {
      throw new RuntimeException("Cannot use resource without declaring it with uses()");
    }

    return storage.get(resource);
  }

  protected <T> void set(Resource<T> resource, T value) throws IOException {

    if (!resources.get(ResourceAction.CREATES).contains(resource)) {
      throw new RuntimeException("Cannot set resource without declaring it with creates()");
    }

    storage.set(resource, value);
  }

  public String fullId() {
    return actionId.resolve();
  }

  protected final void internalExecute(Map<Object, Object> properties) {
    List<ResourceSemaphore> locks = Lists.newArrayList();

    try {

      //  only set properties not explicitly set by the step
      for (Object prop : properties.keySet()) {
        if (!stepProperties.containsKey(prop)) {
          stepProperties.put(prop, properties.get(prop));
        }
      }

      jobPoller = new JobPoller(fullId(), operations, persistence);
      jobPoller.start();

      prepDirs();
      locks = lockReadsFromStores();
      LOG.info("Acquired locks for " + getDatastores(DSAction.READS_FROM));
      LOG.info("Locks " + locks);
      execute();

      jobPoller.shutdown();

    } catch (Throwable t) {
      LOG.fatal("Action " + fullId() + " failed due to Throwable", t);
      throw wrapRuntimeException(t);
    } finally {
      unlock(locks);
      if (jobPoller != null) {
        //  try to refresh info in case of fast failures
        jobPoller.updateRunningJobs();
        jobPoller.shutdown();
      }
    }
  }

  private void unlock(List<ResourceSemaphore> locks) {
    for (ResourceSemaphore lock : locks) {
      lock.release();
    }
  }

  private List<ResourceSemaphore> lockReadsFromStores() {
    List<ResourceSemaphore> locks = Lists.newArrayList();
    if (lockProvider != null) {
      for (DataStore readsFromDatastore : getDatastores(DSAction.READS_FROM)) {
        ResourceSemaphore lock = lockProvider.createLock(readsFromDatastore);
        lock.lock();
        locks.add(lock);
      }
    }
    return locks;
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
                                  CounterFilter counterFilter) {
    this.lockProvider = lockProvider;
    this.persistence = persistence;
    this.storage = storage;
    this.counterFilter = counterFilter;
  }

  protected StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }

  protected FlowBuilder buildFlow(Map<Object, Object> properties) {

    Map<Object, Object> allProps = Maps.newHashMap(stepProperties);
    for (Map.Entry<Object, Object> property : properties.entrySet()) {
      allProps.put(property.getKey(), property.getValue());
    }

    return new FlowBuilder(CascadingHelper.get().getFlowConnector(allProps), getClass());
  }

  protected FlowBuilder buildFlow() {
    return new FlowBuilder(buildFlowConnector(), getClass());
  }

  protected FlowConnector buildFlowConnector() {
    return CascadingHelper.get().getFlowConnector(stepProperties);
  }

  protected void completeWithProgress(RunnableJob job) {
    job.addProperties(stepProperties);
    completeWithProgress(new HadoopOperation(job));
  }

  protected Flow completeWithProgress(FlowBuilder.IFlowClosure flowc) {
    Flow flow = flowc.buildFlow();
    completeWithProgress(new FlowOperation(flow));
    return flow;
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
    operation.start();

    operation.complete();

    //  TODO this is because some things call execute() directly... really want to prevent that eventually
    if(persistence != null) {
      recordCounters(operation);
    }

  }

  private void recordCounters(ActionOperation operation) {

    //  make sure each MR job exists in the DB
    jobPoller.updateRunningJobs();

    for (RunningJob job : operation.listJobs()) {
      String jobId = job.getID().toString();

      try {
        TwoNestedMap<String, String, Long> map = Counters.getCounterMap(job);
        TwoNestedMap<String, String, Long> toRecord = new TwoNestedMap<String, String, Long>();

        for (TwoNestedMap.Entry<String, String, Long> counter : map) {
          String group = counter.getK1();
          String name = counter.getK2();
          if (counterFilter.isRecord(group, name)) {
            toRecord.put(group, name, counter.getValue());
          }
        }

        persistence.markJobCounters(fullId(), jobId, toRecord);

      } catch (IOException e) {
        LOG.error("Failed to capture stats for job: " + jobId);
      }

    }
  }

  protected long getCurrentExecutionId() throws IOException {
    return persistence.getExecutionId();
  }

}
