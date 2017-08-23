package com.rapleaf.cascading_ext.workflow2;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.commons.util.MultiShutdownHook;
import com.liveramp.workflow.formatting.TimeFormatting;
import com.liveramp.workflow_core.BaseWorkflowOptions;
import com.liveramp.workflow_core.ContextStorage;
import com.liveramp.workflow_core.WorkflowUtil;
import com.liveramp.workflow_core.runner.BaseAction;
import com.liveramp.workflow_core.runner.BaseStep;
import com.liveramp.workflow_state.DSAction;
import com.liveramp.workflow_state.DataStoreInfo;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.rollback.RollbackBehavior;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.cascading_ext.workflow2.strategy.ExecuteStrategy;
import com.rapleaf.cascading_ext.workflow2.strategy.RollbackStrategy;

public class BaseWorkflowRunner<Config> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseWorkflowRunner.class);

  private final WorkflowStatePersistence persistence;

  private final Config context;

  private final ResourceManager resourceManager;

  private final ContextStorage storage;

  private final StepExecutor<Config> forwardExecutor;

  private final StepExecutor<Config> rollbackExecutor;

  private final DirectedGraph<BaseStep<Config>, DefaultEdge> dependencyGraph;

  private final NotificationManager notifications;

  private final MultiShutdownHook shutdownHook;

  private final RollbackBehavior rollbackBehavior;

  public interface OnShutdown<Context> {
    public void shutdown(Context context);

    static class NoOp implements OnShutdown {
      @Override
      public void shutdown(Object o) {
        // no op
      }
    }
  }

  public interface OnStepRunnerStart {
    public void onStart();

    static class NoOp implements OnStepRunnerStart {

      @Override
      public void onStart() {
        // no op
      }
    }
  }

  private OnShutdown<Config> onShutdown;

  private boolean alreadyRun;

  public BaseWorkflowRunner(InitializedWorkflow initializedData,
                            Set<? extends BaseStep<Config>> tailSteps,
                            Config config) throws IOException {
    this(initializedData, tailSteps, config, new OnShutdown.NoOp(), new OnStepRunnerStart.NoOp());
  }

  public BaseWorkflowRunner(InitializedWorkflow initializedData,
                            Set<? extends BaseStep<Config>> tailSteps,
                            Config config,
                            OnShutdown<Config> shutdownHook,
                            OnStepRunnerStart onStart) throws IOException {

    BaseWorkflowOptions options = initializedData.getOptions();

    this.context = config;
    this.onShutdown = shutdownHook;
    this.storage = options.getStorage();
    this.resourceManager = initializedData.getManager();
    this.rollbackBehavior = options.getRollBackBehavior();

    WorkflowUtil.setCheckpointPrefixes(tailSteps);
    this.dependencyGraph = WorkflowDiagram.dependencyGraphFromTailSteps(Sets.newHashSet(tailSteps));

    this.persistence = initializedData.prepare(dependencyGraph);

    removeRedundantEdges(dependencyGraph);
    setStepContextObjects(dependencyGraph);

    assertSandbox(options.getSandboxDir());

    this.notifications = new NotificationManager(null, persistence, options.getUrlBuilder());

    this.shutdownHook = initializedData.getShutdownHook();

    this.forwardExecutor = new StepExecutor<Config>(
        new ExecuteStrategy<>(),
        persistence,
        options.getMaxConcurrentSteps(),
        options.getStepPollInterval(),
        dependencyGraph,
        options.getWorkflowJobProperties(),
        initializedData.getShutdownHook(),
        onStart,
        options.getSuccessCallbacks(),
        notifications
    );

    this.rollbackExecutor = new StepExecutor<Config>(
        new RollbackStrategy<>(),
        persistence,
        options.getMaxConcurrentSteps(),
        options.getStepPollInterval(),
        dependencyGraph,
        options.getWorkflowJobProperties(),
        initializedData.getShutdownHook(),
        onStart,
        options.getSuccessCallbacks(),
        new NotificationManager("ROLLBACK", persistence, options.getUrlBuilder())
    );

    // TODO: verify datasources satisfied

  }


  private void setStepContextObjects(DirectedGraph<BaseStep<Config>, DefaultEdge> dependencyGraph) {
    for (BaseStep<Config> step : dependencyGraph.vertexSet()) {
      step.getAction().setOptionObjects(
          this.persistence,
          this.resourceManager,
          this.storage,
          this.context
      );
    }
  }

  private void removeRedundantEdges(DirectedGraph<BaseStep<Config>, DefaultEdge> graph) {
    for (BaseStep<Config> step : graph.vertexSet()) {
      Set<BaseStep<Config>> firstDegDeps = new HashSet<>();
      Set<BaseStep<Config>> secondPlusDegDeps = new HashSet<>();
      for (DefaultEdge edge : graph.outgoingEdgesOf(step)) {
        BaseStep<Config> depStep = graph.getEdgeTarget(edge);
        firstDegDeps.add(depStep);
        getDepsRecursive(depStep, secondPlusDegDeps, graph);
      }

      for (BaseStep<Config> firstDegDep : firstDegDeps) {
        if (secondPlusDegDeps.contains(firstDegDep)) {
          LOG.debug("Found a redundant edge from " + step.getCheckpointToken()
              + " to " + firstDegDep.getCheckpointToken());
          graph.removeAllEdges(step, firstDegDep);
        }
      }
    }
  }

  private void getDepsRecursive(BaseStep<Config> step, Set<BaseStep<Config>> deps, DirectedGraph<BaseStep<Config>, DefaultEdge> graph) {
    for (DefaultEdge edge : graph.outgoingEdgesOf(step)) {
      BaseStep<Config> s = graph.getEdgeTarget(edge);
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

  private void checkStepsSandboxViolation(Collection<DataStoreInfo> dataStores, String sandboxDir) throws IOException {
    if (dataStores != null) {
      for (DataStoreInfo dataStore : dataStores) {
        if (!isSubPath(sandboxDir, dataStore.getPath())) {
          throw new IOException("Step wants to write outside of sandbox \""
              + sandboxDir + "\"" + " into \"" + dataStore.getPath() + "\"");
        }
      }
    }
  }

  private void assertSandbox(String sandboxDir) {
    if (sandboxDir != null) {
      LOG.info("Checking that no action writes outside sandboxDir \"" + sandboxDir + "\"");
      try {
        for (BaseStep<Config> step : getPhsyicalDependencyGraph().vertexSet()) {
          BaseAction stepAction = step.getAction();
          if (stepAction != null) { // TODO: check if this check is necessary, it shouldn't be
            Multimap<DSAction, DataStoreInfo> dsInfo = stepAction.getAllDataStoreInfo();

            checkStepsSandboxViolation(dsInfo.get(DSAction.CREATES), sandboxDir);
            checkStepsSandboxViolation(dsInfo.get(DSAction.CREATES_TEMPORARY), sandboxDir);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
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
    try {

      // Run workflow forwards
      forwardExecutor.doRun();

    } catch (Exception e) {

      //  Rollback, if configured
      if (rollbackBehavior.rollbackOnException(persistence)) {
        rollbackExecutor.doRun();
      }

      throw e;
    } finally {
      LOG.info("Removing shutdown hook");
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
      onShutdown.shutdown(context);
      LOG.info("Timing statistics:\n" + TimeFormatting.getFormattedTimes(dependencyGraph, persistence));
    }
  }


  private DirectedGraph<BaseStep<Config>, DefaultEdge> getPhsyicalDependencyGraph() {
    return dependencyGraph;
  }

}
