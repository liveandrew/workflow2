package com.rapleaf.cascading_ext.workflow2;

import com.liveramp.cascading_ext.counters.Counter;
import com.liveramp.workflow_service.generated.*;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.counters.NestedCounter;
import com.rapleaf.cascading_ext.datastore.DataStore;
import com.rapleaf.cascading_ext.workflow2.state.HdfsCheckpointPersistence;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.webui.WorkflowWebServer;
import com.rapleaf.support.MailerHelper;
import com.rapleaf.support.event_timer.EventTimer;
import com.rapleaf.support.event_timer.TimedEventHelper;
import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Semaphore;

public final class WorkflowRunner {
  private static final Logger LOG = Logger.getLogger(WorkflowRunner.class);

  public static final Set<StepExecuteStatus._Fields> NON_BLOCKING = EnumSet.of(
      StepExecuteStatus._Fields.COMPLETED, StepExecuteStatus._Fields.SKIPPED
  );

  /**
   * Specify this and the system will pick any free port.
   */
  public static final Integer ANY_FREE_PORT = 0;
  private static final String WORKFLOW_EMAIL_SUBJECT_PREFIX = "[WORKFLOW] ";

  private static final String DOC_FILES_ROOT = "com/rapleaf/cascading_ext/workflow2/webui";
  public static final String WORKFLOW_DOCS_PATH = "/var/nfs/mounts/files/flowdoc/";

  private final WorkflowStatePersistence persistence;

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
          try {
            if (NON_BLOCKING.contains(state.getStatus(step).getSetField())) {
              LOG.info("Step " + step.getCheckpointToken()
                  + " was executed successfully in a prior run. Skipping.");
              update(StepExecuteStatus.skipped(new StepSkippedMeta()));
            } else {
              update(StepExecuteStatus.running(new StepRunningMeta()));
              LOG.info("Executing step " + step.getCheckpointToken());
              step.run();
              update(StepExecuteStatus.completed(new StepCompletedMeta()));
            }
          } catch (Throwable e) {
            LOG.fatal("Step " + step.getCheckpointToken() + " failed!", e);

            try {

              StringWriter sw = new StringWriter();
              PrintWriter pw = new PrintWriter(sw);
              e.printStackTrace(pw);

              WorkflowException exception = new WorkflowException(e.getMessage(), sw.toString());
              update(StepExecuteStatus.failed(new StepFailedMeta(exception)));

              //  mark the flow as failed if nothing else has already
              if (!persistence.getFlowStatus().isSetFailed()) {
                ExecuteStatus status = persistence.getFlowStatus();
                status.getActive().setStatus(ActiveStatus.failPending(new FailMeta()));
                persistence.setStatus(status);
              }

            } catch (Exception e2) {
              LOG.fatal("Could not update step " + step.getCheckpointToken() + " to failed! ", e2);
            }
          } finally {
            semaphore.release();
          }
        }
      };
      thread = new Thread(r, "Step Runner for " + step.getCheckpointToken());
      thread.start();
    }

    private void update(StepExecuteStatus status) throws IOException {
      state.updateStatus(step, status);
    }

    public boolean allDependenciesCompleted() {
      for (DefaultEdge edge : dependencyGraph.outgoingEdgesOf(step)) {
        Step dep = dependencyGraph.getEdgeTarget(edge);
        if (!NON_BLOCKING.contains(state.getStatus(dep).getSetField())) {
          return false;
        }
      }
      return true;
    }
  }


  private final String workflowName;
  /**
   * how many components will we allow to execute simultaneously?
   */
  private final int maxConcurrentSteps;
  /**
   * the port to run the status UI on
   */
  // private final Integer webUiPort;

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

  private final Map<String, StepRunner> stepTokenToRunner = new HashMap<String, StepRunner>();

  private final EventTimer timer;

  public static final String SHUTDOWN_MESSAGE_PREFIX = "Incomplete steps remain but a shutdown was requested. The reason was: ";

  private boolean alreadyRun;
  private Integer webUiPort;
  private final String notificationEmails;
  private final Set<NotificationType> enabledNotificationTypes;
  private String sandboxDir;

  public String getSandboxDir() {
    return sandboxDir;
  }

  public void setSandboxDir(String sandboxDir) {
    this.sandboxDir = sandboxDir;
  }

  private WorkflowWebServer webServer;

  public enum NotificationType {
    SUCCESS, FAILURE, SHUTDOWN
  }

  public WorkflowRunner(String workflowName, String checkpointDir, int maxConcurrentSteps, Integer webUiPort, final Step first, Step... rest) {
    this(workflowName,
        checkpointDir,
        maxConcurrentSteps,
        webUiPort,
        combine(first, rest));
  }

  private static HashSet<Step> combine(final Step first, Step... rest) {
    HashSet<Step> s = new HashSet<Step>(Arrays.asList(rest));
    s.add(first);
    return s;
  }

  public WorkflowRunner(String workflowName, String checkpointDir, int maxConcurrentComponents, Integer webUiPort, Set<Step> tailSteps) {
    this(workflowName, checkpointDir, maxConcurrentComponents, webUiPort, tailSteps, null);
  }

  public WorkflowRunner(String workflowName, String checkpointDir, int maxConcurrentComponents, Integer webUiPort, Set<Step> tailSteps, String notificationEmails) {
    this(workflowName, new HdfsCheckpointPersistence(checkpointDir), maxConcurrentComponents, webUiPort, tailSteps, notificationEmails);
  }

  public WorkflowRunner(String workflowName, String checkpointDir, int maxConcurrentComponents, Integer webUiPort, Set<Step> tailSteps, String notificationEmails, boolean deleteCheckpointsOnSuccess) {
    this(workflowName, new HdfsCheckpointPersistence(checkpointDir, deleteCheckpointsOnSuccess), maxConcurrentComponents, webUiPort, tailSteps, notificationEmails);
  }

  public WorkflowRunner(String workflowName, WorkflowStatePersistence persistence, int maxConcurrentComponents, Integer webUiPort, Set<Step> tailSteps, String notificationEmails) {
    this.workflowName = workflowName;
    this.persistence = persistence;
    this.maxConcurrentSteps = maxConcurrentComponents;
    if (webUiPort == null) {
      this.webUiPort = ANY_FREE_PORT;
    } else {
      this.webUiPort = webUiPort;
    }
    this.notificationEmails = notificationEmails;
    if (notificationEmails != null) {
      this.enabledNotificationTypes = EnumSet.allOf(NotificationType.class);
    } else {
      this.enabledNotificationTypes = EnumSet.noneOf(NotificationType.class);
    }

    this.semaphore = new Semaphore(maxConcurrentComponents);

    this.tailSteps = tailSteps;
    this.timer = new EventTimer(workflowName);
    dependencyGraph = WorkflowDiagram.flatDependencyGraphFromTailSteps(tailSteps, timer);


    removeRedundantEdges(dependencyGraph);

    // TODO: verify datasources satisfied

    // prep runners
    for (Step step : dependencyGraph.vertexSet()) {
      StepRunner runner = new StepRunner(step, persistence);
      stepTokenToRunner.put(step.getCheckpointToken(), runner);
      pendingSteps.add(runner);
    }
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
      deps.add(s);
      getDepsRecursive(s, deps, graph);
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
          throw new RuntimeException("Step wants to write outside of sandbox \""
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
          checkStepsSandboxViolation(stepAction.getCreatesDatastores());
          checkStepsSandboxViolation(stepAction.getCreatesTemporaryDatastores());
        }
      }
    }
  }

  public void generateDocs(WorkflowDiagram diagram) {
    String outputPath = "/tmp/flowdoc";
    try {
      String host = InetAddress.getLocalHost().getHostName();
      Set<String> productionHosts = new HashSet<String>(Arrays.asList(
          "ds-jobs.rapleaf.com",
          "s2s-master.rapleaf.com"
      ));
      if (productionHosts.contains(host)) {
        outputPath = WORKFLOW_DOCS_PATH;
      }
    } catch (UnknownHostException e) {
    }
    generateDocs(diagram, outputPath);
  }

  /**
   * Generate HTML docs with the workflow diagram and details about processes and datastores;
   */
  public void generateDocs(WorkflowDiagram wfd, String outputPath) {
    try {

      new File(outputPath).mkdirs();
      File outputFile = new File(outputPath + "/" + workflowName.replaceAll("\\s", "-") + ".html");
      FileWriter fw = new FileWriter(outputFile);
      fw.write(getDocsHtml(wfd, false));
      fw.close();

      copyResources(outputPath);
    } catch (Exception e) {
      LOG.warn("Error generating workflow docs", e);
    }
  }

  public String getDocsHtml(WorkflowDiagram wfd, boolean liveWorkflow) throws IOException {
    Map<String, String> templateFields = new HashMap<String, String>();
    templateFields.put("${workflow_name}", workflowName);
    templateFields.put("${workflow_def}", wfd.getJSWorkflowDefinition(liveWorkflow));
    if (liveWorkflow) {
      String liverWorkflowDef = "liveworkflow = true;\n";
      ExecuteStatus status = persistence.getFlowStatus();
      if (status.isSetActive() && status.getActive().getStatus().isSetShutdownPending()) {
        liverWorkflowDef += "shutdown_reason = \"" + status.getActive().getStatus().getShutdownPending().getCause() + "\"\n";
      }
      templateFields.put("${live_workflow_def}", liverWorkflowDef);
    } else {
      templateFields.put("${live_workflow_def}", "");
    }

    StringBuilder sb = new StringBuilder();
    InputStream is = this.getClass().getClassLoader().getResourceAsStream(DOC_FILES_ROOT + "/workflow_template.html");
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    while ((line = br.readLine()) != null) {
      sb.append(replaceTemplateFields(templateFields, line) + "\n");
    }
    return sb.toString();
  }

  private String replaceTemplateFields(Map<String, String> templateFields, String line) {
    for (Map.Entry<String, String> entry : templateFields.entrySet()) {
      if (line.contains(entry.getKey())) {
        line = line.replace(entry.getKey(), entry.getValue());
      }
    }
    return line;
  }

  private void copyResources(String outputPath) throws IOException {
    InputStream is;
    new File(outputPath + "/js").mkdirs();
    new File(outputPath + "/css").mkdirs();
    new File(outputPath + "/img").mkdirs();
    String[] files = new String[]{
        "js/bootstrap.min.js", "js/dag_layout.js", "js/diagrams2.js", "js/graph.js", "js/jquery.min.js",
        "js/raphael-min.js", "js/workflow_diagram.js",
        "css/bootstrap-responsive.min.css", "css/bootstrap.min.css", "css/workflow_diagram.css",
        "img/glyphicons-halflings-white.png", "img/glyphicons-halflings.png"
    };

    for (String file : files) {
      is = getClass().getClassLoader().getResourceAsStream(DOC_FILES_ROOT + "/" + file);
      OutputStream os = new FileOutputStream(outputPath + "/" + file);
      byte[] buffer = new byte[4096];
      int length;
      while ((length = is.read(buffer)) > 0) {
        os.write(buffer, 0, length);
      }
      os.close();
      is.close();
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
      WorkflowDiagram diagram = new WorkflowDiagram(this);
      generateDocs(diagram);

      LOG.info("Preparing workflow state");

      persistence.prepare(diagram.getDefinition());

      LOG.info("Starting workflow " + getWorkflowName());

      startWebServer();

      runInternal();

      LOG.info("All steps in workflow " + getWorkflowName() + " complete");

      persistence.setStatus(ExecuteStatus.complete(new CompleteMeta()));

      LOG.info("Sending success email");
      sendSuccessEmail();

      LOG.info("Done!");
    } finally {
      shutdownWebServer();
      timer.stop();
      LOG.info("Timing statistics:\n" + TimedEventHelper.toTextSummary(timer));
    }
  }

  private void runInternal() {
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

    // if there are any failures, then the workflow failed. throw an exception.
    if (isFailPending()) {
      String failureMessage = buildFailureMessage();
      sendFailureEmail(failureMessage);
      throw new RuntimeException("One or more steps failed!\n" + failureMessage);
    }

    // nothing failed, but if there are steps that haven't been executed, it's
    // because someone shut down the workflow.
    if (pendingSteps.size() > 0) {
      sendShutdownEmail();

      String reason = getReasonForShutdownRequest();
      persistence.setStatus(ExecuteStatus.shutdown(new ShutdownMeta(reason)));

      throw new RuntimeException(SHUTDOWN_MESSAGE_PREFIX + reason);
    }
  }

  private String buildFailureMessage() {
    int n = 1;
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    int numFailed = 0;
    for (Map.Entry<String, StepExecuteStatus> status : persistence.getAllStepStatuses().entrySet()) {
      if (status.getValue().isSetFailed()) {
        numFailed++;
      }
    }

    for (Map.Entry<String, StepExecuteStatus> status : persistence.getAllStepStatuses().entrySet()) {
      if (status.getValue().isSetFailed()) {
        StepFailedMeta meta = status.getValue().getFailed();
        pw.println("(" + n + "/" + numFailed + ") Step "
            + status.getKey() + " failed with exception: "
            + meta.getCause().getCause());
        pw.println(meta.getCause().getStacktrace());
        n++;
      }
    }
    return sw.toString();
  }

  private void sendSuccessEmail() {
    if (enabledNotificationTypes.contains(NotificationType.SUCCESS)) {
      mail("Succeeded: " + getWorkflowName(), "");
    }
  }

  private void sendFailureEmail(String msg) {
    if (enabledNotificationTypes.contains(NotificationType.FAILURE)) {
      mail("Failed: " + workflowName, msg);
    }
  }

  private void sendShutdownEmail() {
    if (enabledNotificationTypes.contains(NotificationType.SHUTDOWN)) {
      mail("Shutdown requested: " + workflowName, "Reason for shutdown: " + getReasonForShutdownRequest());
    }
  }

  private void mail(String subject, String body) {
    subject = WORKFLOW_EMAIL_SUBJECT_PREFIX + subject;
    try {
      MailerHelper.mail(notificationEmails, subject, body);
    } catch (IOException e) {
      LOG.info("Could not send notification email to: " + notificationEmails);
      LOG.info("subject: " + subject);
      if (!body.isEmpty()) {
        LOG.info("body: " + body);
      }
      throw new RuntimeException(e);
    }
  }

  public boolean isFailPending() {
    ExecuteStatus flowStatus = persistence.getFlowStatus();
    return flowStatus.isSetActive() && flowStatus.getActive().getStatus().isSetFailPending();
  }

  public boolean isShutdownPending() {
    ExecuteStatus flowStatus = persistence.getFlowStatus();
    return flowStatus.isSetActive() && flowStatus.getActive().getStatus().isSetShutdownPending();
  }

  public void disableNotificationType(NotificationType notificationType) {
    enabledNotificationTypes.remove(notificationType);
  }

  public void enableNotificationType(NotificationType notificationType) {
    enabledNotificationTypes.add(notificationType);
  }

  public void requestShutdown(String reason) {
    LOG.info("Shutdown was requested. Running components will be allowed to complete before shutdown.");

    String reasonForShutdown;
    if (reason == null || reason.equals("")) {
      reasonForShutdown = "No reason provided.";
    } else {
      reasonForShutdown = reason;
    }

    ActiveState state = persistence.getFlowStatus().getActive();
    state.setStatus(ActiveStatus.shutdownPending(new ShutdownMeta(reasonForShutdown)));
    ExecuteStatus newStatus = ExecuteStatus.active(state);
    persistence.setStatus(newStatus);
  }

  public String getReasonForShutdownRequest() {
    return persistence.getFlowStatus().getActive().getStatus().getShutdownPending().getCause();
  }

  private void clearFinishedSteps() {
    Iterator<StepRunner> iter = runningSteps.iterator();
    while (iter.hasNext()) {
      StepRunner cr = iter.next();
      LOG.info("Checking persistence for " + cr.step.getCheckpointToken());
      switch (persistence.getStatus(cr.step).getSetField()) {
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
    if (webUiPort != null && webServer != null) {
      webServer.stop();
    }
  }

  private void startWebServer() {
    if (webUiPort != null) {
      webServer = new WorkflowWebServer(this, webUiPort);
      webServer.start();
    }
  }

  public StepExecuteStatus getStepStatus(Step step) {
    return persistence.getStatus(stepTokenToRunner.get(step.getCheckpointToken()).step);
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
