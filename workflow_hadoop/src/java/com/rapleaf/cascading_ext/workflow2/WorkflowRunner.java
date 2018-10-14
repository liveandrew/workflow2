package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import com.liveramp.workflow.backpressure.FlowSubmissionController;
import com.liveramp.workflow.state.WorkflowDbPersistenceFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.megadesk.StoreReaderLocker;
import com.liveramp.workflow_state.InitializedPersistence;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public final class WorkflowRunner extends BaseWorkflowRunner<WorkflowRunner.ExecuteConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowRunner.class);


  public WorkflowRunner(Class klass, Step tail) throws IOException {
    this(klass, new WorkflowDbPersistenceFactory(), tail);
  }

  public WorkflowRunner(Class klass, HadoopWorkflowOptions options, Step tail) throws IOException {
    this(klass, new WorkflowDbPersistenceFactory(), options, tail);
  }

  public WorkflowRunner(Class klass, Set<Step> tailSteps) throws IOException {
    this(klass, new WorkflowDbPersistenceFactory(), tailSteps);
  }

  public WorkflowRunner(Class klass, HadoopWorkflowOptions options, Set<Step> tailSteps) throws IOException {
    this(klass, new WorkflowDbPersistenceFactory(), options, tailSteps);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public WorkflowRunner(HadoopWorkflowOptions options, Step tail) throws IOException {
    this(new WorkflowDbPersistenceFactory(), options, tail);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public WorkflowRunner(HadoopWorkflowOptions options, Set<Step> tailSteps) throws IOException {
    this(new WorkflowDbPersistenceFactory(), options, tailSteps);
  }

  public <K extends InitializedPersistence, W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(String workflowName, WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistence, HadoopWorkflowOptions options, Step tail) throws IOException {
    this(workflowName, persistence, options, Sets.newHashSet(tail));
  }

  public <K extends InitializedPersistence, W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(Class klass, WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistence, Step tail) throws IOException {
    this(klass.getName(), persistence, tail);
  }

  public <K extends InitializedPersistence, W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(Class klass, WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistence, HadoopWorkflowOptions options, final Step first, Step... rest) throws IOException {
    this(klass.getName(), persistence, options, combine(first, rest));
  }

  private static HashSet<Step> combine(final Step first, Step... rest) {
    HashSet<Step> s = new HashSet<>(Arrays.asList(rest));
    s.add(first);
    return s;
  }

  public <K extends InitializedPersistence, W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(
      Class klass,
      WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistence,
      HadoopWorkflowOptions options,
      Set<Step> tailSteps) throws IOException {
    this(klass.getName(), persistence, options, tailSteps);
  }

  public <K extends InitializedPersistence, W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(
      Class klass,
      WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistence,
      Set<Step> tailSteps) throws IOException {
    this(klass, persistence, new ProductionWorkflowOptions(), tailSteps);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public <K extends InitializedPersistence,
      W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(
      WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistence,
      HadoopWorkflowOptions options,
      Set<Step> tailSteps) throws IOException {
    this(persistence.initialize(options), tailSteps);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public <K extends InitializedPersistence, W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(
      WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistence,
      HadoopWorkflowOptions options,
      Step tail) throws IOException {
    this(persistence, options, Sets.newHashSet(tail));
  }

  public <K extends InitializedPersistence,
      W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(
      String workflowName,
      WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistence, Step tail) throws IOException {
    this(workflowName, persistence, Sets.newHashSet(tail));
  }

  public <K extends InitializedPersistence, W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(
      String workflowName,
      WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistence,
      Set<Step> tail) throws IOException {
    this(workflowName, persistence, new ProductionWorkflowOptions(), Sets.newHashSet(tail));
  }

  public <
      K extends InitializedPersistence,
      W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(
      String workflowName, WorkflowPersistenceFactory<Step, K, HadoopWorkflowOptions, W> persistenceFactory,
      HadoopWorkflowOptions options,
      Set<Step> tailSteps) throws IOException {

    this(persistenceFactory.initialize(workflowName, options), tailSteps);
  }

  public <K extends InitializedPersistence,
      W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(InitializedWorkflow<Step, K, HadoopWorkflowOptions> initializedData, Step tail) throws IOException {
    this(initializedData, Sets.newHashSet(tail));
  }

  public <K extends InitializedPersistence, W extends InitializedWorkflow<Step, K, HadoopWorkflowOptions>> WorkflowRunner(
      W initializedData,
      Set<Step> tailSteps) throws IOException {

    super(initializedData, tailSteps,
        new ExecuteConfig(
            initializedData.getOptions().getLockProvider().create(),
            initializedData.getOptions().getFlowSubmissionController()
        ),
        new OnShutdown<ExecuteConfig>() {
          @Override
          public void shutdown(ExecuteConfig context) {
            LOG.info("Shutting down lock provider");
            context.getLockProvider().shutdown();
          }
        },
        new OnStepRunnerStart() {
          @Override
          public void onStart() {
            CascadingHelper.get().getJobConf();
          }
        }
    );

  }

  public static class ExecuteConfig {
    private StoreReaderLocker lockProvider;
    private FlowSubmissionController submissionController;

    public ExecuteConfig(StoreReaderLocker lockProvider, FlowSubmissionController submissionController) {
      this.lockProvider = lockProvider;
      this.submissionController = submissionController;
    }

    public ExecuteConfig(StoreReaderLocker lockProvider) {
      this.lockProvider = lockProvider;
      this.submissionController = new FlowSubmissionController.SubmitImmediately();
    }

    public StoreReaderLocker getLockProvider() {
      return lockProvider;
    }

    public FlowSubmissionController getSubmissionController() {
      return submissionController;
    }
  }

}
