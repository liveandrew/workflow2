package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import cascading.flow.Flow;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.cascading_ext.megadesk.ResourceSemaphore;
import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.datastore.internal.DataStoreBuilder;
import com.rapleaf.cascading_ext.workflow2.action_operations.FlowOperation;
import com.rapleaf.cascading_ext.workflow2.action_operations.HadoopOperation;

public abstract class Action {
  private static final Logger LOG = Logger.getLogger(Action.class);

  private final String checkpointToken;
  private final String tmpRoot;

  private final Set<DataStore> readsFromDatastores = new HashSet<DataStore>();
  private final Set<DataStore> createsDatastores = new HashSet<DataStore>();
  private final Set<DataStore> createsTemporaryDatastores = new HashSet<DataStore>();
  private final Set<DataStore> writesToDatastores = new HashSet<DataStore>();

  private StoreReaderLockProvider lockProvider;

  private String lastStatusMessage = "";

  private int pctComplete;

  private long startTimestamp;
  private long endTimestamp;

  private List<ActionOperation> operations = new ArrayList<ActionOperation>();

  private FileSystem fs;

  private DataStoreBuilder builder = null;

  public Action(String checkpointToken) {
    this.checkpointToken = checkpointToken;
    this.tmpRoot = null;
  }

  public Action(String checkpointToken, String tmpRoot) {
    this.checkpointToken = checkpointToken;
    this.tmpRoot = tmpRoot + "/" + checkpointToken + "-tmp-stores";
    this.builder = new DataStoreBuilder(getTmpRoot());
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

  protected void readsFrom(DataStore store) {
    readsFromDatastores.add(store);
  }

  protected void creates(DataStore store) {
    createsDatastores.add(store);
  }

  protected void createsTemporary(DataStore store) {
    createsTemporaryDatastores.add(store);
  }

  protected void writesTo(DataStore store) {
    writesToDatastores.add(store);
  }

  protected abstract void execute() throws Exception;

  public DataStoreBuilder builder() {
    return builder;
  }

  protected final void internalExecute() {
    List<ResourceSemaphore> locks = Lists.newArrayList();
    try {
      startTimestamp = System.currentTimeMillis();
      prepDirs();
      locks = lockReadsFromStores();
      LOG.info("Aquired locks for " + readsFromDatastores);
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
      for (DataStore readsFromDatastore : readsFromDatastores) {
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

  @SuppressWarnings("PMD.BlacklistedMethods") //  temporary hopefully, until we get more cluster space
  private void prepDirs() throws Exception {
    FileSystem fs = FileSystemHelper.getFS();
    for (Set<DataStore> datastores : Arrays.asList(createsDatastores, createsTemporaryDatastores)) {
      for (DataStore datastore : datastores) {
        String uri = new URI(datastore.getPath()).getPath();
        Path path = new Path(datastore.getPath());
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

  public Set<DataStore> getReadsFromDatastores() {
    return readsFromDatastores;
  }

  public Set<DataStore> getCreatesDatastores() {
    return createsDatastores;
  }

  public Set<DataStore> getWritesToDatastores() {
    return writesToDatastores;
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

  protected void setPercentComplete(int pctComplete) {
    this.pctComplete = Math.min(Math.max(0, pctComplete), 100);
  }

  public int getPercentComplete() {
    return this.pctComplete;
  }

  public Map<String, String> getStatusLinks() {
    Map<String, String> linkToName = new LinkedHashMap<String, String>();

    for (ActionOperation operation : operations) {
      linkToName.putAll(operation.getSubStepStatusLinks());
    }

    return linkToName;
  }

  /**
   * Given a running Flow, compute what percent complete it is. The percent of
   * completion is defined as the number of FlowSteps that have completed
   * divided by the total number of FlowSteps times the maxPct value. This
   * normalizes the percent complete to the max possible percent of the total
   * component's work represented by this one Flow.
   *
   * @param operation
   * @param maxPct
   * @return
   */
  static int getActionProgress(ActionOperation operation, int maxPct) {
    try {
      return operation.getProgress(maxPct);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void setLockProvider(StoreReaderLockProvider lockProvider) {
    this.lockProvider = lockProvider;
  }

  protected StoreReaderLockProvider getLockProvider() {
    return lockProvider;
  }


  private class OperationProgressMonitor extends Thread {
    private boolean _keepRunning;
    private final ActionOperation _operation;
    private final int _startPct;
    private final int _maxPct;

    public OperationProgressMonitor(ActionOperation operation, int startPct, int maxPct) {
      super("OperationProgressMonitor for " + operation.getName());
      setDaemon(true);
      _keepRunning = true;
      _operation = operation;
      _startPct = startPct;
      _maxPct = maxPct;
    }

    @Override
    public void run() {
      while (_keepRunning) {
        try {
          setPercentComplete(_startPct + getActionProgress(_operation, _maxPct - _startPct));
          sleep(15000);
        } catch (InterruptedException e) {
          // just want to stop sleeping and pay attention
        }
      }
    }

    public void stopAndInterrupt() {
      _keepRunning = false;
      interrupt();
    }
  }

  /**
   * Complete the provided operation while monitoring and reporting its progress.
   * This method will call setPercentComplete with values between 0 and 100
   * incrementally based on the completion of the operation's steps.
   *
   * @param operation
   */
  protected void completeWithProgress(ActionOperation operation) {
    completeWithProgress(operation, 0, 100);
  }

  protected void completeWithProgress(Flow flow) {
    completeWithProgress(new FlowOperation(flow));
  }

  protected void completeWithProgress(Flow flow, int startPct, int maxPct) {
    completeWithProgress(new FlowOperation(flow), startPct, maxPct);
  }

  protected void completeWithProgress(RunnableJob job) {
    completeWithProgress(new HadoopOperation(job));
  }

  /**
   * Complete the provided ActionOperation while monitoring and reporting its progress.
   * This method will call setPercentComplete with values between startPct and
   * maxPct incrementally based on the completion of the ActionOperation's steps.
   *
   * @param operation
   * @param startPct
   * @param maxPct
   */
  protected void completeWithProgress(ActionOperation operation, int startPct, int maxPct) {
    runningFlow(operation);
    operation.start();

    OperationProgressMonitor fpm = new OperationProgressMonitor(operation, startPct, maxPct);
    fpm.start();

    operation.complete();
    fpm.stopAndInterrupt();
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  public Set<DataStore> getCreatesTemporaryDatastores() {
    return createsTemporaryDatastores;
  }
}
