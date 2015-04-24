package com.rapleaf.cascading_ext.workflow2;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;

import com.google.common.collect.Sets;
import org.apache.hadoop.mapred.JobConf;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.counters.Counter;
import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.util.HadoopJarUtil;
import com.liveramp.cascading_ext.util.HadoopProperties;
import com.liveramp.cascading_ext.util.NestedProperties;
import com.liveramp.commons.collections.nested_map.TwoNestedCountingMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertMessages;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipient;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.java_support.event_timer.EventTimer;
import com.liveramp.java_support.event_timer.TimedEventHelper;
import com.liveramp.types.workflow.LiveWorkflowMeta;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.MapreduceCounter;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.workflow.DSAction;
import com.rapleaf.db_schemas.rldb.workflow.StepState;
import com.rapleaf.db_schemas.rldb.workflow.StepStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowStatePersistence;

public final class WorkflowRunner {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowRunner.class);

  /**
   * Specify this and the system will pick any free port.j
   */
  public static final String WORKFLOW_EMAIL_SUBJECT_TAG = "WORKFLOW";
  public static final String ERROR_EMAIL_SUBJECT_TAG = "ERROR";

  private static final String JOB_PRIORITY_PARAM = "mapred.job.priority";
  private static final String JOB_POOL_PARAM = "mapred.queue.name";

  private final WorkflowStatePersistence persistence;
  private final StoreReaderLockProvider lockProvider;
  private final ContextStorage storage;
  private final int stepPollInterval;

  //  set this if something fails in a step (outside user-code) so we don't keep trying to start steps
  private List<Exception> internalErrors = new CopyOnWriteArrayList<Exception>();

  private NestedProperties workflowJobProperties;

  /**
   * how many components will we allow to execute simultaneously?
   */
  private final int maxConcurrentSteps;

  private final DirectedGraph<Step, DefaultEdge> dependencyGraph;

  /**
   * semaphore used to control the max number of running components
   */
  private final Semaphore semaphore;

  /**
   * components that haven't yet been started
   */
  private final Set<StepRunner> pendingSteps = new HashSet<StepRunner>();
  /**
   * components that have been started and not yet finished
   */
  private final Set<StepRunner> runningSteps = new HashSet<StepRunner>();
  /**
   * started and completed successfully
   */
  private final Set<StepRunner> completedSteps = new HashSet<StepRunner>();

  private final EventTimer timer;

  private final Thread shutdownHook;

  private boolean alreadyRun;
  private final AlertsHandler alertsHandler;
  private final Set<WorkflowRunnerNotification> enabledNotifications;
  private final CounterFilter counterFilter;
  private final ResourceManager<?, ?> resourceManager;
  private String sandboxDir;

  public String getSandboxDir() {
    return sandboxDir;
  }

  public void setSandboxDir(String sandboxDir) {
    this.sandboxDir = sandboxDir;
  }

  public WorkflowRunner(Class klass, Step tail) {
    this(klass, new DbPersistenceFactory(), tail);
  }

  public WorkflowRunner(Class klass, WorkflowOptions options, Step tail) {
    this(klass, new DbPersistenceFactory(), options, tail);
  }

  public WorkflowRunner(Class klass, Set<Step> tailSteps) {
    this(klass, new DbPersistenceFactory(), tailSteps);
  }

  public WorkflowRunner(Class klass, WorkflowOptions options, Set<Step> tailSteps) {
    this(klass, new DbPersistenceFactory(), options, tailSteps);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public WorkflowRunner(WorkflowOptions options, Step tail) {
    this(new DbPersistenceFactory(), options, tail);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public WorkflowRunner(WorkflowOptions options, Set<Step> tailSteps) {
    this(new DbPersistenceFactory(), options, tailSteps);
  }


  public WorkflowRunner(String workflowName, WorkflowPersistenceFactory persistence, WorkflowOptions options, Step tail) {
    this(workflowName, persistence, options, Sets.newHashSet(tail));
  }

  public WorkflowRunner(Class klass, WorkflowPersistenceFactory persistence, Step tail) {
    this(klass.getName(), persistence, tail);
  }

  public WorkflowRunner(Class klass, WorkflowPersistenceFactory persistence, WorkflowOptions options, final Step first, Step... rest) {
    this(klass.getName(), persistence, options, combine(first, rest));
  }

  public WorkflowRunner(Class klass, WorkflowPersistenceFactory persistence, WorkflowOptions options, Set<Step> tailSteps) {
    this(klass.getName(), persistence, options, tailSteps);
  }

  public WorkflowRunner(Class klass, WorkflowPersistenceFactory persistence, Set<Step> tailSteps) {
    this(klass, persistence, new ProductionWorkflowOptions(), tailSteps);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public WorkflowRunner(WorkflowPersistenceFactory persistence, WorkflowOptions options, Set<Step> tailSteps) {
    this(getName(options), persistence, options, tailSteps);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public WorkflowRunner(WorkflowPersistenceFactory persistence, WorkflowOptions options, Step tail) {
    this(getName(options), persistence, options, Sets.newHashSet(tail));
  }

  public WorkflowRunner(String workflowName, WorkflowPersistenceFactory persistence, Step tail) {
    this(workflowName, persistence, Sets.newHashSet(tail));
  }

  public WorkflowRunner(String workflowName, WorkflowPersistenceFactory persistence, Set<Step> tail) {
    this(workflowName, persistence, new ProductionWorkflowOptions(), Sets.newHashSet(tail));
  }

  public WorkflowRunner(String workflowName, WorkflowPersistenceFactory persistenceFactory, WorkflowOptions options, Set<Step> tailSteps) {

    //  TODO we can move name into WorkflowOptions and do this verification when setting it there.  eliminate contructors here
    verifyName(workflowName, options);

    this.maxConcurrentSteps = options.getMaxConcurrentSteps();
    this.alertsHandler = options.getAlertsHandler();
    this.counterFilter = options.getCounterFilter();
    this.enabledNotifications = options.getEnabledNotifications().get();
    this.semaphore = new Semaphore(maxConcurrentSteps);
    this.timer = new EventTimer(workflowName);
    this.lockProvider = options.getLockProvider();
    this.storage = options.getStorage();
    this.workflowJobProperties = options.getWorkflowJobProperties();
    this.stepPollInterval = options.getStepPollInterval();
    this.resourceManager = options.getResourceManager();

    WorkflowUtil.setCheckpointPrefixes(tailSteps);
    this.dependencyGraph = WorkflowDiagram.dependencyGraphFromTailSteps(tailSteps, timer);

    this.persistence = persistenceFactory.prepare(dependencyGraph,
        workflowName,
        options.getScopeIdentifier(),
        options.getAppType(),
        getHostName(),
        System.getProperty("user.name"),
        findDefaultValue(JOB_POOL_PARAM, "default"),
        findDefaultValue(JOB_PRIORITY_PARAM, "NORMAL"),
        System.getProperty("user.dir"),
        HadoopJarUtil.getLanuchJarName()
    );

    linkPersistence();

    removeRedundantEdges(dependencyGraph);
    setStepContextObjects(dependencyGraph);

    // TODO: verify datasources satisfied

    // prep runners
    for (Step step : dependencyGraph.vertexSet()) {
      StepRunner runner = new StepRunner(step, this.persistence);
      pendingSteps.add(runner);
    }

    this.shutdownHook = new Thread(new ShutdownHook());
  }

  private void linkPersistence() {
    if (resourceManager != null) {
      try {
        resourceManager.linkResourceRoot(persistence);
      } catch (IOException e) {
        throw new RuntimeException("Could not link resource root to persistence: " + e);
      }
    }
  }

  private void verifyName(String name, WorkflowOptions options) {
    AppType appType = options.getAppType();
    if (appType != null) {
      if (!appType.name().equals(name)) {
        throw new RuntimeException("Workflow name cannot conflict with AppType name!");
      }
    } else {
      for (AppType a : AppType.values()) {
        if (a.name().equals(name)) {
          throw new RuntimeException("Provided workflow name " + name + " is already an AppType");
        }
      }
    }
  }

  private static String getName(WorkflowOptions options) {
    AppType appType = options.getAppType();
    if (appType == null) {
      throw new RuntimeException("AppType must be set in WorkflowOptions!");
    }
    return appType.name();
  }

  private static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private void setStepContextObjects(DirectedGraph<Step, DefaultEdge> dependencyGraph) {
    for (Step step : dependencyGraph.vertexSet()) {
      step.getAction().setOptionObjects(
          this.lockProvider,
          this.persistence,
          this.storage,
          this.counterFilter,
          this.resourceManager
      );
    }
  }

  private static HashSet<Step> combine(final Step first, Step... rest) {
    HashSet<Step> s = new HashSet<Step>(Arrays.asList(rest));
    s.add(first);
    return s;
  }

  private void removeRedundantEdges(DirectedGraph<Step, DefaultEdge> graph) {
    for (Step step : graph.vertexSet()) {
      Set<Step> firstDegDeps = new HashSet<Step>();
      Set<Step> secondPlusDegDeps = new HashSet<Step>();
      for (DefaultEdge edge : graph.outgoingEdgesOf(step)) {
        Step depStep = graph.getEdgeTarget(edge);
        firstDegDeps.add(depStep);
        getDepsRecursive(depStep, secondPlusDegDeps, graph);
      }

      for (Step firstDegDep : firstDegDeps) {
        if (secondPlusDegDeps.contains(firstDegDep)) {
          LOG.debug("Found a redundant edge from " + step.getCheckpointToken()
              + " to " + firstDegDep.getCheckpointToken());
          graph.removeAllEdges(step, firstDegDep);
        }
      }
    }
  }

  private void getDepsRecursive(Step step, Set<Step> deps, DirectedGraph<Step, DefaultEdge> graph) {
    for (DefaultEdge edge : graph.outgoingEdgesOf(step)) {
      Step s = graph.getEdgeTarget(edge);
      boolean isNew = deps.add(s);
      if (isNew) {
        getDepsRecursive(s, deps, graph);
      }
    }
  }

  private static String canonicalPath(String path) throws IOException {
    return new File(path).getCanonicalPath();
  }

  private static boolean isSubPath(String parentPath, String childPath) throws IOException {
    return canonicalPath(childPath).startsWith(canonicalPath(parentPath));
  }

  public WorkflowStatePersistence getPersistence() {
    return persistence;
  }

  private void checkStepsSandboxViolation(Set<DataStore> dataStores) throws IOException {
    if (dataStores != null) {
      for (DataStore dataStore : dataStores) {
        if (!isSubPath(getSandboxDir(), dataStore.getPath())) {
          throw new IOException("Step wants to write outside of sandbox \""
              + getSandboxDir() + "\"" + " into \"" + dataStore.getPath() + "\"");
        }
      }
    }
  }

  public void checkStepsSandboxViolation(Collection<Step> steps) throws IOException {
    if (getSandboxDir() != null) {
      for (Step step : steps) {
        Action stepAction = step.getAction();
        if (stepAction != null) { // TODO: check if this check is necessary, it shouldn't be
          checkStepsSandboxViolation(stepAction.getDatastores(DSAction.CREATES));
          checkStepsSandboxViolation(stepAction.getDatastores(DSAction.CREATES_TEMPORARY));
        }
      }
    }
  }

  /**
   * Execute the workflow.
   *
   * @throws IOException
   */
  public synchronized void run() throws IOException {
    if (alreadyRun) {
      throw new IllegalStateException("The workflow is already running (or finished)!");
    }
    alreadyRun = true;
    timer.start();
    try {
      LOG.info("Checking that no action goes outside sandboxDir \"" + getSandboxDir() + "\"");
      checkStepsSandboxViolation(getPhsyicalDependencyGraph().vertexSet());

      // Notify
      LOG.info(getStartMessage());
      // Note: start email after web server so that UI is functional
      sendStartEmail();

      persistence.markWorkflowStarted();

      Runtime.getRuntime().addShutdownHook(shutdownHook);

      // Run internal
      runInternal();

      // Notify success
      sendSuccessEmail();
      LOG.info(getSuccessMessage());
    } finally {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
      timer.stop();
      LOG.info("Timing statistics:\n" + TimedEventHelper.toTextSummary(timer));
    }
  }

  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      try {
        LOG.info("Process killed, updating persistence.");
        for (StepRunner runningStep : runningSteps) {
          persistence.markStepFailed(
              runningStep.step.getCheckpointToken(),
              new RuntimeException("Workflow process killed!")
          );
        }
        persistence.markWorkflowStopped();
        LOG.info("Finished cleaning up status");
      } catch (Exception e) {
        LOG.info("Failed to cleanly shutdown", e);
      }
    }
  }

  private void runInternal() throws IOException {
    // keep trying to start new components for as long as we are allowed and
    // there are components left to start
    while (shouldKeepStartingSteps() && pendingSteps.size() > 0) {
      // process any completed/failed steps
      clearFinishedSteps();

      // we check again here because we want to make sure we don't bother to
      // try acquiring the semaphore if we should shut down.
      if (!shouldKeepStartingSteps()) {
        break;
      }

      // acquire semaphore
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        LOG.debug("Interrupted waiting to acquire semaphore.", e);
        continue;
      }

      // we do this check *again* because this time, we might have been told
      // to shut down while we were waiting on the semaphore.
      if (!shouldKeepStartingSteps()) {
        // VERY important to release the semaphore here, otherwise the "wait
        // for completion" thing below will block indefinitely.
        semaphore.release();
        break;
      }

      // check if there are any startable steps
      StepRunner startableStep = getStartableStep(pendingSteps);

      if (startableStep == null) {
        // we didn't find any step to start, so wait a little.
        // important to release the semaphore here, because we're going to go
        // back around the loop.
        semaphore.release();
        try {
          Thread.sleep(stepPollInterval);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted waiting for step to become ready", e);
        }
      } else {
        // start one startable
        runningSteps.add(startableStep);
        pendingSteps.remove(startableStep);
        startStep(startableStep);

        // note that we explicitly don't release the semaphore here. this is
        // because the actual step runner thread will release it when it's
        // complete (or failed).
      }
    }

    // acquire all the permits on the semaphore. this will guarantee that zero
    // components are running.
    try {
      semaphore.acquire(maxConcurrentSteps);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for running steps to complete!", e);
    }

    // there are now no running steps. clear the finished ones again in
    // case someone failed.
    clearFinishedSteps();

    persistence.markWorkflowStopped();

    // if there are any failures, then the workflow failed. throw an exception.
    if (isFailPending()) {
      String failureMessage = buildStepsFailureMessage();
      sendFailureEmail(failureMessage);
      throw new RuntimeException(getFailureMessage() + "\n" + failureMessage);
    }

    //  something internal to WorkflowRunner failed.
    if (!internalErrors.isEmpty()) {
      LOG.error("WorkflowRunner has encountered an internal error");
      sendInternalErrorMessage();
      throw new RuntimeException(getFailureMessage() + " internal WorkflowRunner error");
    }

    // nothing failed, but if there are steps that haven't been executed, it's
    // because someone shut down the workflow.
    if (pendingSteps.size() > 0) {
      String reason = getReasonForShutdownRequest();
      sendShutdownEmail(reason);
      throw new RuntimeException(getShutdownMessage(reason));
    }
  }

  private void sendInternalErrorMessage() throws IOException {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.write("WorkflowRunner failed with an internal error.  Manual cleanup may be necessary.");

    for (Exception error : internalErrors) {
      pw.append(error.getMessage())
          .append("\n");
      error.printStackTrace(pw);
      pw.append("---------------------\n");
    }

    mail(getFailureMessage(), pw.toString(), AlertRecipients.engineering(AlertSeverity.ERROR));

  }

  private String buildStepsFailureMessage() throws IOException {
    int n = 1;
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    int numFailed = 0;
    Map<String, StepState> statuses = persistence.getStepStatuses();
    for (Map.Entry<String, StepState> status : statuses.entrySet()) {
      if (status.getValue().getStatus() == StepStatus.FAILED) {
        numFailed++;
      }
    }

    for (Map.Entry<String, StepState> status : statuses.entrySet()) {
      StepState value = status.getValue();
      if (value.getStatus() == StepStatus.FAILED) {
        pw.println("(" + n + "/" + numFailed + ") Step "
            + status.getKey() + " failed with exception: "
            + value.getFailureMessage());
        pw.println(value.getFailureTrace());
        n++;
      }
    }
    return sw.toString();
  }

  public LiveWorkflowMeta getMeta() throws IOException {
    return new LiveWorkflowMeta()
        .set_uuid(persistence.getId())
        .set_name(persistence.getName())
        .set_host(InetAddress.getLocalHost().getHostName())
        .set_username(System.getProperty("user.name"))
        .set_working_dir(System.getProperty("user.dir"))
        .set_jar(HadoopJarUtil.getLanuchJarName())
        .set_start_time(getTimer().getEventStartTime());
  }

  //  TODO use AttemptStatus when migration done
  public boolean isFailPending() throws IOException {

    for (Map.Entry<String, StepState> entry : persistence.getStepStatuses().entrySet()) {
      if (entry.getValue().getStatus() == StepStatus.FAILED) {
        return true;
      }
    }

    return false;
  }


  private void sendStartEmail() throws IOException {
    if (enabledNotifications.contains(WorkflowRunnerNotification.START)) {
      mail(getStartMessage(), AlertRecipients.engineering(AlertSeverity.INFO));
    }
  }

  private void sendSuccessEmail() throws IOException {
    if (enabledNotifications.contains(WorkflowRunnerNotification.SUCCESS)) {
      mail(getSuccessMessage(), AlertRecipients.engineering(AlertSeverity.INFO));
    }
  }

  private void sendFailureEmail(String msg) throws IOException {
    if (enabledNotifications.contains(WorkflowRunnerNotification.FAILURE)) {
      mail(getFailureMessage(), msg, AlertRecipients.engineering(AlertSeverity.ERROR));
    }
  }

  private void sendShutdownEmail(String cause) throws IOException {
    if (enabledNotifications.contains(WorkflowRunnerNotification.SHUTDOWN)) {
      mail(getShutdownMessage(cause), AlertRecipients.engineering(AlertSeverity.INFO));
    }
  }

  private String getStartMessage() throws IOException {
    return "Started: " + persistence.getName();
  }

  private String getSuccessMessage() throws IOException {
    return "Succeeded: " + persistence.getName();
  }

  private String getFailureMessage() throws IOException {
    return "[" + ERROR_EMAIL_SUBJECT_TAG + "] " + "Failed: " + persistence.getName();
  }

  private String getShutdownMessage(String reason) throws IOException {
    return "Shutdown requested: " + persistence.getName() + ". Reason: " + reason;
  }

  private void mail(String subject, AlertRecipient recipient) throws IOException {
    mail(subject, "", recipient);
  }

  private void mail(String subject, String body, AlertRecipient recipient) throws IOException {
    alertsHandler.sendAlert(
        AlertMessages.builder(subject)
            .setBody(body)
            .addToDefaultTags(WORKFLOW_EMAIL_SUBJECT_TAG)
            .build(),
        recipient
    );
  }

  private String findDefaultValue(String property, String defaultValue) {

    //  fall back to static jobconf props if not set elsewhere
    JobConf jobconf = CascadingHelper.get().getJobConf();

    if (workflowJobProperties.isSetProperty(property)) {
      return (String)workflowJobProperties.getProperty(property);
    }

    String value = jobconf.get(property);

    if (value != null) {
      return value;
    }

    //  only really expect in tests
    return defaultValue;
  }

  private String getReasonForShutdownRequest() throws IOException {
    return persistence.getShutdownRequest();
  }

  private void clearFinishedSteps() throws IOException {
    Iterator<StepRunner> iter = runningSteps.iterator();
    while (iter.hasNext()) {
      StepRunner cr = iter.next();
      //LOG.info("Checking persistence for " + cr.step.getCheckpointToken());
      switch (persistence.getState(cr.step.getCheckpointToken()).getStatus()) {
        case COMPLETED:
        case SKIPPED:
          completedSteps.add(cr);
          iter.remove();
          break;
        case FAILED:
          iter.remove();
          break;
      }
    }
  }

  private void startStep(StepRunner stepRunner) {
    stepRunner.start();
  }

  private StepRunner getStartableStep(Set<StepRunner> pendingSteps) throws IOException {
    for (StepRunner cr : pendingSteps) {
      if (cr.allDependenciesCompleted()) {
        return cr;
      }
    }
    return null;
  }

  private boolean shouldKeepStartingSteps() throws IOException {
    return !isFailPending() && persistence.getShutdownRequest() == null && internalErrors.isEmpty();
  }

  public DirectedGraph<Step, DefaultEdge> getPhsyicalDependencyGraph() {
    return dependencyGraph;
  }

  public EventTimer getTimer() {
    return timer;
  }


  public TwoNestedMap<String, String, Long> getFlatCounters(IRlDb db) throws IOException {
    long executionId = persistence.getExecutionId();

    TwoNestedCountingMap<String, String> counters = new TwoNestedCountingMap<String, String>(0l);
    for (WorkflowAttempt attempt : db.workflowExecutions().find(executionId).getWorkflowAttempt()) {
      for (StepAttempt step : attempt.getStepAttempt()) {
        for (MapreduceJob job : step.getMapreduceJobs()) {
          for (MapreduceCounter counter : job.getMapreduceCounters()) {
            counters.incrementAndGet(counter.getGroup(), counter.getName(), counter.getValue());
          }
        }
      }
    }

    return counters;

  }

  @Deprecated
  private List<NestedCounter> getCounters() {
    try {
      // we don't know what stage of execution we are in when this is called
      // so get an up-to-date list of counters each time
      List<NestedCounter> counters = new ArrayList<NestedCounter>();
      for (StepRunner sr : completedSteps) {
        for (NestedCounter c : sr.step.getCounters()) {
          counters.add(c);
        }
      }
      return counters;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Deprecated
  public Map<String, Map<String, Long>> getCounterMap() {
    // we don't know what stage of execution we are in when this is called
    // so get an up-to-date list of counters each time
    List<NestedCounter> counters = this.getCounters();
    Map<String, Map<String, Long>> counterMap = new HashMap<String, Map<String, Long>>();
    for (NestedCounter nestedCounter : counters) {
      Counter counter = nestedCounter.getCounter();
      if (counterMap.get(counter.getGroup()) == null) {
        counterMap.put(counter.getGroup(), new HashMap<String, Long>());
      }
      counterMap.get(counter.getGroup()).put(counter.getName(), counter.getValue());
    }
    return counterMap;
  }

  /**
   * StepRunner keeps track of some extra state for each component, as
   * well as manages the actual execution thread. Note that it is itself *not*
   * a Thread.
   */
  private final class StepRunner {
    public final Step step;
    private final WorkflowStatePersistence state;
    public Thread thread;

    public StepRunner(Step c, WorkflowStatePersistence state) {
      this.step = c;
      this.state = state;
    }

    private NestedProperties buildInheritedProperties() throws IOException {
      HadoopProperties.Builder uiPropertiesBuilder = new HadoopProperties.Builder();
      String priority = persistence.getPriority();
      String pool = persistence.getPool();

      if (priority != null) {
        uiPropertiesBuilder.setProperty(JOB_PRIORITY_PARAM, priority, true);
      }

      if (pool != null) {
        uiPropertiesBuilder.setProperty(JOB_POOL_PARAM, pool, true);
      }

      return new NestedProperties(workflowJobProperties, uiPropertiesBuilder.build());
    }

    public void start() {
      CascadingHelper.get().getJobConf();
      Runnable r = new Runnable() {
        @Override
        public void run() {
          String stepToken = step.getCheckpointToken();
          try {
            if (StepStatus.NON_BLOCKING.contains(state.getState(stepToken).getStatus())) {
              LOG.info("Step " + stepToken + " was executed successfully in a prior run. Skipping.");
            } else {

              persistence.markStepRunning(stepToken);

              LOG.info("Executing step " + stepToken);
              step.run(buildInheritedProperties());

              persistence.markStepCompleted(stepToken);
            }
          } catch (Throwable e) {

            LOG.error("Step " + stepToken + " failed!", e);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);

            try {
              persistence.markStepFailed(stepToken, e);
            } catch (Exception e2) {
              LOG.error("Could not update step " + stepToken + " to failed! ", e2);
              internalErrors.add(e2);
            }

          } finally {
            semaphore.release();
          }
        }
      };
      thread = new Thread(r, "Step Runner for " + step.getCheckpointToken());
      thread.start();
    }

    public boolean allDependenciesCompleted() throws IOException {
      for (DefaultEdge edge : dependencyGraph.outgoingEdgesOf(step)) {
        Step dep = dependencyGraph.getEdgeTarget(edge);
        if (!StepStatus.NON_BLOCKING.contains(state.getState(dep.getCheckpointToken()).getStatus())) {
          return false;
        }
      }
      return true;
    }
  }
}
