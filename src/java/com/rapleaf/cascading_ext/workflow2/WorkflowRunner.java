package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.rapleaf.cascading_ext.workflow2.webui.WorkflowWebServer;
import com.rapleaf.support.event_timer.EventTimer;
import com.rapleaf.support.event_timer.TimedEventHelper;

public final class WorkflowRunner {
  private static final Logger LOG = Logger.getLogger(WorkflowRunner.class);
  
  /**
   * StepRunner keeps track of some extra state for each component, as
   * well as manages the actual execution thread. Note that it is itself *not*
   * a Thread.
   */
  private final class StepRunner {
    public final Step step;
    public StepStatus status;
    public Thread thread;
    public Throwable failureCause;
    
    public StepRunner(Step c) {
      this.step = c;
      this.status = StepStatus.WAITING;
    }
    
    public void start() {
      Runnable r = new Runnable() {
        @Override
        public void run() {
          try {
            if (checkpointExists()) {
              LOG.info("Step " + step.getCheckpointToken()
                  + " was executed successfully in a prior run. Skipping.");
              status = StepStatus.SKIPPED;
            } else {
              status = StepStatus.RUNNING;
              LOG.info("Executing step " + step.getCheckpointToken());
              step.run();
              writeCheckpoint();
              status = StepStatus.COMPLETED;
            }
          } catch (Throwable e) {
            LOG.fatal("Step " + step.getCheckpointToken() + " failed!", e);
            failureCause = e;
            status = StepStatus.FAILED;
          } finally {
            semaphore.release();
          }
        }
      };
      thread = new Thread(r, "Step Runner for " + step.getCheckpointToken());
      thread.start();
    }
    
    protected void writeCheckpoint() throws IOException {
      LOG.info("Writing out checkpoint token for " + step.getCheckpointToken());
      String tokenPath = checkpointDir + "/" + step.getCheckpointToken();
      if (!fs.createNewFile(new Path(tokenPath))) {
        throw new IOException("Couldn't create checkpoint file " + tokenPath);
      }
      LOG.debug("Done writing checkpoint token for " + step.getCheckpointToken());
    }
    
    protected boolean checkpointExists() throws IOException {
      return fs.exists(new Path(checkpointDir + "/" + step.getCheckpointToken()));
    }
    
    public boolean allDependenciesCompleted() {
      for (DefaultEdge edge : dependencyGraph.outgoingEdgesOf(step)) {
        Step dep = dependencyGraph.getEdgeTarget(edge);
        if (!runnerFor(dep).isCompleted()) {
          return false;
        }
      }
      return true;
    }
    
    private boolean isCompleted() {
      return status == StepStatus.COMPLETED || status == StepStatus.SKIPPED;
    }
  }
  
  private final String workflowName;
  /**
   * where should we write checkpoints
   */
  private final String checkpointDir;
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
   * started and failed
   */
  private final Set<StepRunner> failedSteps = new HashSet<StepRunner>();
  /**
   * started and completed successfully
   */
  private final Set<StepRunner> completedSteps = new HashSet<StepRunner>();
  
  private final Map<String, StepRunner> stepTokenToRunner = new HashMap<String, StepRunner>();
  
  private boolean shutdownPending = false;
  
  private String reasonForShutdownRequest = null;
  
  private final EventTimer timer;
  
  public static final String SHUTDOWN_MESSAGE_PREFIX = "Incomplete steps remain but a shutdown was requested. The reason was: ";
  
  private boolean alreadyRun;
  private FileSystem fs;
  private Integer webUiPort;
  private WorkflowWebServer webServer;
  
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
  
  public StepRunner runnerFor(Step step) {
    return stepTokenToRunner.get(step.getCheckpointToken());
  }
  
  public WorkflowRunner(String workflowName, String checkpointDir, int maxConcurrentComponents, Integer webUiPort, Set<Step> tailSteps) {
    this.workflowName = workflowName;
    this.checkpointDir = checkpointDir;
    this.maxConcurrentSteps = maxConcurrentComponents;
    this.webUiPort = webUiPort;
    
    this.semaphore = new Semaphore(maxConcurrentComponents);
    
    this.tailSteps = tailSteps;
    this.timer = new EventTimer(workflowName);
    dependencyGraph = WorkflowDiagram.flatDependencyGraphFromTailSteps(tailSteps, timer);
    
    removeRedundantEdges(dependencyGraph);
    
    // TODO: verify datasources satisfied
    
    // prep runners
    for (Step step : dependencyGraph.vertexSet()) {
      StepRunner runner = new StepRunner(step);
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
      fs = FileSystem.get(new Configuration());
      
      LOG.info("Creating checkpoint dir " + checkpointDir);
      fs.mkdirs(new Path(checkpointDir));
      
      LOG.info("Starting workflow " + getWorkflowName());
      
      startWebServer();
      
      runInternal();
      
      LOG.info("All steps in workflow " + getWorkflowName() + " complete");
      
      LOG.debug("Deleting checkpoint dir " + checkpointDir);
      fs.delete(new Path(checkpointDir), true);
      
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
        // we didn't find any ste[ to start, so wait a little.
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
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
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
    
    // if there are any failures, the the workflow failed. throw an exception.
    if (failedSteps.size() > 0) {
      int n = 1;
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      for (StepRunner c : failedSteps) {
        pw.println("(" + n + "/" + failedSteps.size() + ") Step "
            + c.step.getCheckpointToken() + " failed with exception: "
            + c.failureCause.getMessage());
        c.failureCause.printStackTrace(pw);
        n++ ;
      }
      String wholeMessage = sw.toString();
      throw new RuntimeException("One or more steps failed!\n" + wholeMessage);
    }
    
    // nothing failed, but if there are steps that haven't been executed, it's
    // because someone shut the workflow down.
    if (pendingSteps.size() > 0) {
      throw new RuntimeException(SHUTDOWN_MESSAGE_PREFIX + getReasonForShutdownRequest());
    }
  }
  
  public boolean isShutdownPending() {
    return shutdownPending;
  }
  
  public void requestShutdown(String reason) {
    LOG.info("Shutdown was requested. Running components will be allowed to complete before shutdown.");
    this.shutdownPending = true;
    
    if (reason == null || reason.equals("")) {
      this.reasonForShutdownRequest = "No reason provided.";
    } else {
      this.reasonForShutdownRequest = reason;
    }
  }
  
  public String getReasonForShutdownRequest() {
    return reasonForShutdownRequest;
  }
  
  private void clearFinishedSteps() {
    Iterator<StepRunner> iter = runningSteps.iterator();
    while (iter.hasNext()) {
      StepRunner cr = iter.next();
      switch (cr.status) {
        case COMPLETED:
        case SKIPPED:
          completedSteps.add(cr);
          iter.remove();
          break;
        case FAILED:
          failedSteps.add(cr);
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
    return failedSteps.isEmpty() && !isShutdownPending();
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
  
  public StepStatus getStepStatus(Step step) {
    return stepTokenToRunner.get(step.getCheckpointToken()).status;
  }
  
  public String getWorkflowName() {
    return workflowName;
  }
  
  public String getCheckpointDir() {
    return checkpointDir;
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
}
