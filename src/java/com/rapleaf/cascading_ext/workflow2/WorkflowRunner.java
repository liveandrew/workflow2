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
import java.util.UUID;
import java.util.concurrent.Semaphore;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.cascading_ext.counters.Counter;
import com.liveramp.cascading_ext.megadesk.StoreReaderLockProvider;
import com.liveramp.cascading_ext.util.HadoopJarUtil;
import com.liveramp.java_support.alerts_handler.AlertMessages;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipient;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;
import com.liveramp.types.workflow.LiveWorkflowMeta;
import com.liveramp.workflow_service.generated.StepExecuteStatus;
import com.liveramp.workflow_service.generated.StepFailedMeta;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.registry.WorkflowRegistry;
import com.rapleaf.cascading_ext.workflow2.state.HdfsCheckpointPersistence;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.stats.StepStatsRecorder;
import com.rapleaf.support.Rap;
import com.rapleaf.support.event_timer.EventTimer;
import com.rapleaf.support.event_timer.TimedEventHelper;

public final class WorkflowRunner {
  private static final Logger LOG = Logger.getLogger(WorkflowRunner.class);
  private final StepStatsRecorder statsRecorder;

  /**
   * Specify this and the system will pick any free port.j
   */
  public static final Integer ANY_FREE_PORT = 0;
  public static final String WORKFLOW_EMAIL_SUBJECT_TAG = "WORKFLOW";
  public static final String ERROR_EMAIL_SUBJECT_TAG = "ERROR";

  private static final String JOB_PRIORITY_PARAM = "mapred.job.priority";
  private static final String JOB_POOL_PARAM = "mapred.queue.name";

  private final WorkflowStatePersistence persistence;
  private final StoreReaderLockProvider lockProvider;
  private final ContextStorage storage;
  private final WorkflowRegistry registry;

  private Map<Object, Object> workflowJobProperties = Maps.newHashMap();

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

    public void start() {
      CascadingHelper.get().getJobConf();
      Runnable r = new Runnable() {
        @Override
        public void run() {
          String stepToken = step.getCheckpointToken();
          try {
            if (WorkflowStatePersistence.NON_BLOCKING.contains(state.getStatus(stepToken).getSetField())) {
              LOG.info("Step " + stepToken +  " was executed successfully in a prior run. Skipping.");
              persistence.markStepSkipped(stepToken);
            } else {

              persistence.markStepRunning(stepToken);

              LOG.info("Executing step " + stepToken);
              step.run(storage, Lists.newArrayList(statsRecorder), workflowJobProperties);

              persistence.markStepCompleted(stepToken);
            }
          } catch (Throwable e) {
            LOG.fatal("Step " + stepToken + " failed!", e);

            try {

              StringWriter sw = new StringWriter();
              PrintWriter pw = new PrintWriter(sw);
              e.printStackTrace(pw);

              persistence.markStepFailed(stepToken, e);

            } catch (Exception e2) {
              LOG.fatal("Could not update step " + stepToken + " to failed! ", e2);
            }
          } finally {
            semaphore.release();
          }
        }
      };
      thread = new Thread(r, "Step Runner for " + step.getCheckpointToken());
      thread.start();
    }

    public boolean allDependenciesCompleted() {
      for (DefaultEdge edge : dependencyGraph.outgoingEdgesOf(step)) {
        Step dep = dependencyGraph.getEdgeTarget(edge);
        if (!WorkflowStatePersistence.NON_BLOCKING.contains(state.getStatus(dep.getCheckpointToken()).getSetField())) {
          return false;
        }
      }
      return true;
    }
  }

  private final String workflowName;

  private final String workflowUUID;

  /**
   * how many components will we allow to execute simultaneously?
   */
  private final int maxConcurrentSteps;

  private final DirectedGraph<Step, DefaultEdge> dependencyGraph;
  private final Set<Step> tailSteps;

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

  private boolean alreadyRun;
  private final AlertsHandler alertsHandler;
  private final Set<WorkflowRunnerNotification> enabledNotifications;
  private String sandboxDir;

  public String getSandboxDir() {
    return sandboxDir;
  }

  public void setSandboxDir(String sandboxDir) {
    this.sandboxDir = sandboxDir;
  }

  private WorkflowWebServer webServer;

  public WorkflowRunner(String workflowName, String checkpointDir, Set<Step> tailSteps) {
    this(workflowName, checkpointDir, new ProductionWorkflowOptions(), tailSteps);
  }

  public WorkflowRunner(String workflowName, String checkpointDir, final Step first, final Step... rest) {
    this(workflowName, checkpointDir, new ProductionWorkflowOptions(), first, rest);
  }

  public WorkflowRunner(String workflowName, String checkpointDir, WorkflowOptions options, Set<Step> tailSteps) {
    this(
        workflowName,
        new HdfsCheckpointPersistence(checkpointDir),
        options,
        tailSteps);
  }

  public WorkflowRunner(String workflowName, String checkpointDir, WorkflowOptions options, final Step first, Step... rest) {
    this(workflowName, checkpointDir, options, combine(first, rest));
  }

  @Deprecated
  public WorkflowRunner(String workflowName, String checkpointDir, int maxConcurrentSteps, final Step first, Step... rest) {
    this(workflowName,
        checkpointDir,
        new ProductionWorkflowOptions().setMaxConcurrentSteps(maxConcurrentSteps),
        combine(first, rest));
  }

  public WorkflowRunner(String workflowName, WorkflowStatePersistence persistence, WorkflowOptions options, Set<Step> tailSteps) {
    this.workflowUUID = Hex.encodeHexString(Rap.uuidToBytes(UUID.randomUUID()));
    this.workflowName = workflowName;
    this.persistence = persistence;
    this.maxConcurrentSteps = options.getMaxConcurrentSteps();
    this.statsRecorder = options.getStatsRecorder().makeRecorder(workflowName);
    this.alertsHandler = options.getAlertsHandler();
    this.enabledNotifications = options.getEnabledNotifications().get();
    this.semaphore = new Semaphore(maxConcurrentSteps);
    this.tailSteps = tailSteps;
    this.timer = new EventTimer(workflowName);
    this.lockProvider = options.getLockProvider();
    this.storage = options.getStorage();
    this.workflowJobProperties = options.getWorkflowJobProperties();
    this.registry = options.getRegistry();
    this.dependencyGraph = WorkflowDiagram.flatDependencyGraphFromTailSteps(tailSteps, timer);

    this.persistence.prepare(WorkflowDiagram.getDefinition(dependencyGraph, workflowName));

    removeRedundantEdges(dependencyGraph);
    setLockProvider(dependencyGraph);

    // TODO: verify datasources satisfied

    // prep runners
    for (Step step : dependencyGraph.vertexSet()) {
      StepRunner runner = new StepRunner(step, persistence);
      pendingSteps.add(runner);
    }
  }

  public String getWorkflowUUID() {
    return workflowUUID;
  }

  private void setLockProvider(DirectedGraph<Step, DefaultEdge> dependencyGraph) {
    for (Step step : dependencyGraph.vertexSet()) {
      step.getAction().setLockProvider(this.lockProvider);
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
          checkStepsSandboxViolation(stepAction.getDatastores(Action.DSAction.CREATES));
          checkStepsSandboxViolation(stepAction.getDatastores(Action.DSAction.CREATES_TEMPORARY));
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

      LOG.info("Generating workflow docs");

      LOG.info("Preparing workflow state");

      // Notify
      LOG.info(getStartMessage());
      startWebServer();
      registry.register(workflowUUID, getMeta());
      // Note: start email after web server so that UI is functional
      sendStartEmail();

      // Run internal
      runInternal();

      // Notify success
      sendSuccessEmail();
      LOG.info(getSuccessMessage());
    } finally {
      shutdownWebServer();
      registry.deregister();
      timer.stop();
      LOG.info("Timing statistics:\n" + TimedEventHelper.toTextSummary(timer));
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
          Thread.sleep(100);
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

    if (statsRecorder != null) {
      try {
        statsRecorder.stop();
      } catch (Exception e) {
        //  don't want to interrupt the rest
      }
    }

    // if there are any failures, then the workflow failed. throw an exception.
    if (isFailPending()) {
      String failureMessage = buildStepsFailureMessage();
      sendFailureEmail(failureMessage);
      throw new RuntimeException(getFailureMessage() + "\n" + failureMessage);
    }

    // nothing failed, but if there are steps that haven't been executed, it's
    // because someone shut down the workflow.
    if (pendingSteps.size() > 0) {
      String reason = getReasonForShutdownRequest();
      sendShutdownEmail(reason);
      throw new RuntimeException(getShutdownMessage(reason));
    }
  }

  private String buildStepsFailureMessage() {
    int n = 1;
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    int numFailed = 0;
    Map<String, StepExecuteStatus> statuses = persistence.getFlowStatus().getStepStatuses();
    for (Map.Entry<String, StepExecuteStatus> status : statuses.entrySet()) {
      if (status.getValue().is_set_failed()) {
        numFailed++;
      }
    }

    for (Map.Entry<String, StepExecuteStatus> status : statuses.entrySet()) {
      if (status.getValue().is_set_failed()) {
        StepFailedMeta meta = status.getValue().get_failed();
        pw.println("(" + n + "/" + numFailed + ") Step "
            + status.getKey() + " failed with exception: "
            + meta.get_cause().get_cause());
        pw.println(meta.get_cause().get_stacktrace());
        n++;
      }
    }
    return sw.toString();
  }

  public LiveWorkflowMeta getMeta() throws UnknownHostException {
    return new LiveWorkflowMeta()
        .set_uuid(getWorkflowUUID())
        .set_name(getWorkflowName())
        .set_host(InetAddress.getLocalHost().getHostName())
        .set_port(getWebServer().getBoundPort())
        .set_username(System.getProperty("user.name"))
        .set_working_dir(System.getProperty("user.dir"))
        .set_jar(HadoopJarUtil.getLanuchJarName())
        .set_start_time(getTimer().getEventStartTime());
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

  private String getStartMessage() {
    return "Started: " + getWorkflowName();
  }

  private String getSuccessMessage() {
    return "Succeeded: " + getWorkflowName();
  }

  private String getFailureMessage() {
    return "[" + ERROR_EMAIL_SUBJECT_TAG + "] " + "Failed: " + getWorkflowName();
  }

  private String getShutdownMessage(String reason) {
    return "Shutdown requested: " + getWorkflowName() + ". Reason: " + reason;
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

  protected WorkflowWebServer getWebServer() {
    return webServer;
  }

  public boolean isFailPending() {
    WorkflowStatePersistence.WorkflowState state = persistence.getFlowStatus();

    for (Map.Entry<String, StepExecuteStatus> entry : state.getStepStatuses().entrySet()) {
      if(entry.getValue().is_set_failed()){
        return true;
      }
    }

    return false;
  }

  public boolean isShutdownPending() {
    return persistence.getFlowStatus().getShutdownRequest() != null;
  }

  public void disableNotification(WorkflowRunnerNotification workflowRunnerNotification) {
    enabledNotifications.remove(workflowRunnerNotification);
  }

  public void enableNotification(WorkflowRunnerNotification workflowRunnerNotification) {
    enabledNotifications.add(workflowRunnerNotification);
  }

  public void requestShutdown(String reason) {
    LOG.info("Shutdown was requested. Running components will be allowed to complete before shutdown.");

    String reasonForShutdown;
    if (reason == null || reason.equals("")) {
      reasonForShutdown = "No reason provided.";
    } else {
      reasonForShutdown = reason;
    }

    persistence.markShutdownRequested(reasonForShutdown);
  }

  public void setPriority(String priority){
    LOG.info("Setting new default priority: "+priority);

    //  just so we fail loudly if not valid
    JobPriority.valueOf(priority);

    workflowJobProperties.put(JOB_PRIORITY_PARAM, priority);
  }

  public void setPool(String pool){
    LOG.info("Setting new pool: "+pool);

    workflowJobProperties.put(JOB_POOL_PARAM, pool);
  }

  public String getPriority(){
    return (String) getProperty(JOB_PRIORITY_PARAM);
  }

  public String getPool(){
    return (String) getProperty(JOB_POOL_PARAM);
  }

  private Object getProperty(String property){

    //  fall back to static jobconf props if not set elsewhere
    JobConf jobconf = CascadingHelper.get().getJobConf();

    if(workflowJobProperties.containsKey(property)){
      return workflowJobProperties.get(property);
    }

    Map<Object, Object> defaultProps = CascadingHelper.get().getDefaultProperties();
    if(defaultProps.containsKey(property)){
      return defaultProps.get(property);
    }

    return jobconf.get(property);
  }


  public String getReasonForShutdownRequest() {
    return persistence.getFlowStatus().getShutdownRequest();
  }

  private void clearFinishedSteps() {
    Iterator<StepRunner> iter = runningSteps.iterator();
    while (iter.hasNext()) {
      StepRunner cr = iter.next();
      //LOG.info("Checking persistence for " + cr.step.getCheckpointToken());
      switch (persistence.getStatus(cr.step.getCheckpointToken()).getSetField()) {
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

  private StepRunner getStartableStep(Set<StepRunner> pendingSteps) {
    for (StepRunner cr : pendingSteps) {
      if (cr.allDependenciesCompleted()) {
        return cr;
      }
    }
    return null;
  }

  private boolean shouldKeepStartingSteps() {
    return !isFailPending() && !isShutdownPending();
  }

  private void shutdownWebServer() {
    if (webServer != null) {
      webServer.stop();
    }
  }

  private void startWebServer() {
    webServer = new WorkflowWebServer(this, ANY_FREE_PORT);
    webServer.start();
  }

  public StepExecuteStatus getStepStatus(Step step) {
    return persistence.getStatus(step.getCheckpointToken());
  }

  public String getWorkflowName() {
    return workflowName;
  }

  public int getMaxConcurrentSteps() {
    return maxConcurrentSteps;
  }

  public DirectedGraph<Step, DefaultEdge> getPhsyicalDependencyGraph() {
    return dependencyGraph;
  }

  public Set<Step> getTailSteps() {
    return tailSteps;
  }

  public EventTimer getTimer() {
    return timer;
  }

  public List<NestedCounter> getCounters() {
    // we don't know what stage of execution we are in when this is called
    // so get an up-to-date list of counters each time
    List<NestedCounter> counters = new ArrayList<NestedCounter>();
    for (StepRunner sr : completedSteps) {
      for (NestedCounter c : sr.step.getCounters()) {
        counters.add(c.addParentEvent(workflowName));
      }
    }
    return counters;
  }

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
}
