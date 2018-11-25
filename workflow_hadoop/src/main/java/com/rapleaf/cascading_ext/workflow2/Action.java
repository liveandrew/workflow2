package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
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
import com.liveramp.cascading_ext.megadesk.ILockManager;
import com.liveramp.cascading_ext.megadesk.IStoreReaderLocker;
import com.liveramp.cascading_ext.resource.ReadResource;
import com.liveramp.cascading_ext.resource.Resource;
import com.liveramp.cascading_tools.jobs.TrackedFlow;
import com.liveramp.cascading_tools.jobs.TrackedOperation;
import com.liveramp.commons.collections.properties.NestedProperties;
import com.liveramp.workflow.backpressure.FlowSubmissionController;
import com.liveramp.workflow_core.OldResource;
import com.liveramp.workflow2.workflow_hadoop.TmpDirFilter;
import com.liveramp.workflow_core.runner.BaseAction;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.DataStoreInfo;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.counter.verifier.TemplateTapFiles;
import com.rapleaf.cascading_ext.workflow2.flow_closure.FlowRunner;

public abstract class Action extends BaseAction<WorkflowRunner.ExecuteConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(Action.class);

  private final HdfsActionContext context;

  private ILockManager lockManager;
  private FlowSubmissionController controller;
  private CascadingUtil cascadingUtil;
  private TmpDirFilter tmpDirFilter;

  private final Multimap<DSAction, DataStore> datastores = HashMultimap.create();


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
    super(checkpointToken, properties);
    this.context = new HdfsActionContext(tmpRoot, checkpointToken);
  }

  protected FileSystem getFS() throws IOException {
    return context.getFS();
  }

  public final String getTmpRoot() {
    return context.getTmpRoot();
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
    Preconditions.checkNotNull(store, "Cannot mark a null datastore as used!");
    datastores.put(action, store);
  }

  @Override
  protected final void preExecute() throws Exception {
    //  only set properties not explicitly set by the step

    prepDirs();

    lockManager.lockConsumeStart();

  }

  @Override
  protected final void postExecute() {
    lockManager.release();
  }

  private Set<DataStore> getDatastores(DSAction... actions) {
    Set<DataStore> stores = Sets.newHashSet();

    for (DSAction dsAction : actions) {
      stores.addAll(datastores.get(dsAction));
    }

    return stores;
  }

  @Override
  public Multimap<DSAction, DataStoreInfo> getAllDataStoreInfo() {

    Multimap<DSAction, DataStoreInfo> stores = HashMultimap.create();

    for (Map.Entry<DSAction, DataStore> entry : datastores.entries()) {
      stores.put(entry.getKey(), new DataStoreInfo(
          entry.getValue().getName(),
          entry.getClass().getName(),
          entry.getValue().getPath()
      ));
    }
    return stores;
  }


  @SuppressWarnings("PMD.BlacklistedMethods") //  temporary hopefully, until we get more cluster space
  private void prepDirs() throws Exception {
    for (DataStore ds : getDatastores(DSAction.CREATES, DSAction.CREATES_TEMPORARY)) {
      String uri = new URI(ds.getPath()).getPath();
      Path path = new Path(ds.getPath());
      FileSystem fs = FileSystemHelper.getFileSystemForPath(path);
      Boolean trashEnabled = TrashHelper.isEnabled();

      if (fs.exists(path)) {
        // delete if tmp store, or if no trash is enabled
        if (tmpDirFilter.isSkipTrash(path) || !trashEnabled) {
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


  @Override
  protected void initialize(WorkflowRunner.ExecuteConfig context) {
    this.lockManager = context.getLockProvider()
        .createManager(getDatastores(DSAction.READS_FROM))
        .lockProcessStart();
    this.controller = context.getSubmissionController();
    this.cascadingUtil = context.getCascadingUtil();
    this.tmpDirFilter = context.getTmpDirFilter();
  }

  protected IStoreReaderLocker getLockProvider() {
    return getConfig().getLockProvider();
  }

  protected FlowBuilder buildFlow(Map<Object, Object> properties) {
    return new FlowBuilder(buildFlowConnector(getInheritedProperties(properties)), getClass());
  }

  protected Map<Object, Object> getInheritedProperties() {
    return getInheritedProperties(Maps.newHashMap());
  }

  protected Map<Object, Object> getInheritedProperties(Map<Object, Object> childProperties) {
    return new NestedProperties(childProperties, false).override(getCombinedProperties()).getPropertiesMap();
  }

  public FlowBuilder buildFlow() {
    return buildFlow(Maps.newHashMap());
  }

  protected FlowConnector buildFlowConnector() {
    return cascadingUtil.getFlowConnector(getCombinedProperties().getPropertiesMap());
  }

  private FlowConnector buildFlowConnector(Map<Object, Object> properties) {

    return CascadingUtil.buildFlowConnector(
        new JobConf(),
        getPersister(),
        properties,
        cascadingUtil.getIntermediateSchemeClass(),
        cascadingUtil.resolveFlowStepStrategies(),
        cascadingUtil.getInvalidPropertyValues()
    );
  }

  public Flow completeWithProgress(FlowBuilder.IFlowClosure flowc) {
    return completeWithProgress(flowc, false);
  }

  protected Configuration getConfiguration() {
    JobConf conf = new JobConf();
    Map<Object, Object> properties = getInheritedProperties();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      conf.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return conf;
  }

  protected Map<Object, Object> asProperties(Configuration conf){
    Map<Object, Object> properties = Maps.newHashMap();
    Iterator<Map.Entry<String, String>> iter = conf.iterator();
    while(iter.hasNext()){
      Map.Entry<String, String> entry = iter.next();
      properties.put(entry.getKey(), entry.getValue());
    }
    return properties;
  }

  //  TODO sweep when we figure out cascading npe (prolly upgrade past 2.5.1)
  protected Flow completeWithProgress(FlowBuilder.IFlowClosure flowc, boolean skipCompleteListener) {
    Flow flow = flowc.buildFlow();

    Runnable cleanupCallback = () -> {
    };
    try {
      cleanupCallback = blockUntilSubmissionAllowed(flow);

      TrackedOperation tracked = new TrackedFlow(flow, skipCompleteListener);
      completeWithProgress(tracked);
    } finally {
      cleanupCallback.run();
    }

    return flow;
  }

  private Runnable blockUntilSubmissionAllowed(Flow flow) {
    Optional<String> statusMessageSafe = getStatusMessageSafe();
    String oldMessage = statusMessageSafe.orElse("");
    String newMessage = oldMessage + " Blocking on Submission Controller";
    setStatusMessageSafe(newMessage);
    Runnable cleanupCallback = () -> {
    };
    if (flow.getConfig() instanceof Configuration) {
      cleanupCallback = controller.blockUntilSubmissionAllowed((Configuration)flow.getConfig());
    } else {
      LOG.error("Config is not of type Configuration. Type is instead " + flow.getConfig().getClass().getCanonicalName());
    }
    setStatusMessageSafe(oldMessage);
    return cleanupCallback;
  }


  public JobPersister getPersister() {
    return new WorkflowJobPersister(
        getPersistence(),
        getActionId().resolve(),
        Lists.<WorkflowJobPersister.CounterVerifier>newArrayList(new TemplateTapFiles())
    );
  }

  private transient final List<TrackedOperation> runningJobs = Lists.newArrayList();

  public void completeWithProgress(TrackedOperation tracked) {
    JobPersister persister = getPersister();

    runningJobs.add(tracked);
    tracked.complete(
        persister,
        isFailOnCounterFetch()
    );
    runningJobs.remove(tracked);
  }

  @Override
  public void stop() throws InterruptedException {
    super.stop();

    //  unlikely that many are running in practice, but can't hurt
    List<Thread> killThreads = Lists.newArrayList();
    for (TrackedOperation runningJob : runningJobs) {
      Thread kill = new Thread(runningJob::stop);
      kill.start();
      killThreads.add(kill);
    }

    for (Thread killThread : killThreads) {
      killThread.join();
    }

  }

  public interface TrackedOperationClosure {
    void complete(TrackedOperation operation);
  }

  protected TrackedOperationClosure completeOperationClosure() {
    return this::completeWithProgress;
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

  //  everything we feel like exposing to pre-execute hooks in CA2.  I don't really love that it's here, but this way
  //  we don't have to make these methods public.  there should be a cleaner way but I can't think of it.
  public class PreExecuteContext {

    public <T> T get(OldResource<T> resource) throws IOException {
      return Action.this.get(resource);
    }

    public <T> T get(ReadResource<T> resource) {
      return Action.this.get(resource);
    }

    public void setStatus(String status) throws IOException {
      Action.this.setStatusMessage(status);
    }

    public void setStatusCallback(StatusCallback callback) throws IOException {
      Action.this.setStatusCallback(callback);
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
