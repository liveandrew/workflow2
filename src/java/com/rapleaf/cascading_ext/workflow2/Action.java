package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import org.apache.log4j.Logger;

import cascading.flow.Flow;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.cascading_ext.megadesk.ResourceSemaphore;
import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.workflow2.action.NoOpAction;
import com.rapleaf.cascading_ext.workflow2.action_operations.FlowOperation;
import com.rapleaf.cascading_ext.workflow2.action_operations.HadoopOperation;

public abstract class Action {
  private static final Logger LOG = Logger.getLogger(Action.class);

  private final String checkpointToken;
  private final String tmpRoot;

  public static enum DSAction {

    //  input
    READS_FROM,
    CONSUMES,

    //  output
    CREATES,
    CREATES_TEMPORARY,
    WRITES_TO;

    @Override
    public String toString() {
      return super.toString().toLowerCase();
    }
  }

  private final Multimap<DSAction, DataStore> datastores = HashMultimap.create();

  //  it's tempting to reuse DSAction for this, but I think some DSActions have no parallel for in-memory resources
  //  which actually make sense...
  public static enum ResourceAction {
    CREATES,
    USES
  }

  private final Multimap<ResourceAction, Resource> resources = HashMultimap.create();


  private StoreReaderLockProvider lockProvider;

  private String lastStatusMessage = "";

  private long startTimestamp;
  private long endTimestamp;
  private Map<Object, Object> stepProperties;

  private List<ActionOperation> operations = new ArrayList<ActionOperation>();

  private FileSystem fs;

  private DataStoreBuilder builder = null;
  private ContextStorage storage;

  public Action(String checkpointToken) {
    this(checkpointToken, Maps.newHashMap());
  }

  public Action(String checkpointToken, Map<Object, Object> properties) {
    this.checkpointToken = checkpointToken;
    this.tmpRoot = null;
    this.stepProperties = properties;
  }

  public Action(String checkpointToken, String tmpRoot) {
    this(checkpointToken, tmpRoot, Maps.newHashMap());
  }

  public Action(String checkpointToken, String tmpRoot, Map<Object, Object> properties) {
    this.checkpointToken = checkpointToken;
    this.tmpRoot = tmpRoot + "/" + checkpointToken + "-tmp-stores";
    this.builder = new DataStoreBuilder(getTmpRoot());
    this.stepProperties = properties;
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
    return new Resource<T>(checkpointToken + "__" + name);
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

  protected final void internalExecute(ContextStorage storage, Map<Object, Object> properties) {
    List<ResourceSemaphore> locks = Lists.newArrayList();
    try {
      this.startTimestamp = System.currentTimeMillis();
      this.storage = storage;

      //  only set properties not explicitly set by the step
      for (Object prop : properties.keySet()) {
        if (!stepProperties.containsKey(prop)) {
          stepProperties.put(prop, properties.get(prop));
        }
      }

      prepDirs();
      locks = lockReadsFromStores();
      LOG.info("Aquired locks for " + getDatastores(DSAction.READS_FROM));
      LOG.info("Locks " + locks);
      execute();
    } catch (Throwable t) {
      LOG.fatal("Action " + checkpointToken + " failed due to Throwable", t);
      throw wrapRuntimeException(t);
    } finally {
      endTimestamp = System.currentTimeMillis();
      unlock(locks);
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

  public static RuntimeException wrapRuntimeException(Throwable t) {
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

  public String getCheckpointToken() {
    return checkpointToken;
  }

  public final String getTmpRoot() {
    if (tmpRoot == null) {
      throw new RuntimeException("Temp root not set!");
    }
    return tmpRoot;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [checkpointToken=" + checkpointToken + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((checkpointToken == null) ? 0 : checkpointToken.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Action other = (Action)obj;
    if (checkpointToken == null) {
      if (other.checkpointToken != null) {
        return false;
      }
    } else if (!checkpointToken.equals(other.checkpointToken)) {
      return false;
    }
    return true;
  }

  /**
   * Set an application-specific status message to display. This will be
   * visible in the logs as well as through the UI. This is a great place to
   * report progress or choices.
   *
   * @param statusMessage
   */
  protected void setStatusMessage(String statusMessage) {
    LOG.info("Status Message: " + statusMessage);
    lastStatusMessage = statusMessage;
  }

  public String getStatusMessage() {
    return lastStatusMessage;
  }

  public Map<String, String> getStatusLinks() {
    Map<String, String> linkToName = new LinkedHashMap<String, String>();

    for (ActionOperation operation : operations) {
      linkToName.putAll(operation.getSubStepStatusLinks());
    }

    return linkToName;
  }

  public void setLockProvider(StoreReaderLockProvider lockProvider) {
    this.lockProvider = lockProvider;
  }

  protected StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }

  protected FlowBuilder buildFlow(Map<Object, Object> properties) {

    Map<Object, Object> allProps = Maps.newHashMap(stepProperties);
    for (Map.Entry<Object, Object> property : properties.entrySet()) {
      allProps.put(property.getKey(), property.getValue());
    }

    return new FlowBuilder(CascadingHelper.get().getFlowConnector(allProps));
  }

  protected FlowBuilder buildFlow() {
    return new FlowBuilder(CascadingHelper.get().getFlowConnector(stepProperties));
  }

  @Deprecated
  //  call buildFlow() instead of CascadingHelper.get().getFlowConnector
  protected void completeWithProgress(Flow flow) {
    completeWithProgress(new FlowOperation(flow));
  }

  protected void completeWithProgress(RunnableJob job) {
    completeWithProgress(new HadoopOperation(job));
  }

  protected Flow completeWithProgress(FlowBuilder.FlowClosure flowc) {
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
  protected void completeWithProgress(ActionOperation operation) {
    runningFlow(operation);
    operation.start();

    operation.complete();
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  public static Action optional(Action action, boolean isEnabled) {
    if(isEnabled) {
      return action;
    } else {
      return new NoOpAction("skip-" + action.getCheckpointToken());
    }
  }
}
