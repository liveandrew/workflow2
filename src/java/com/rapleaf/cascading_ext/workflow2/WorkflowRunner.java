package com.rapleaf.cascading_ext.workflow2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.megadesk.StoreReaderLocker;
import com.liveramp.workflow_core.runner.BaseStep;
import com.liveramp.workflow_state.InitializedPersistence;
import com.rapleaf.cascading_ext.CascadingHelper;
import com.rapleaf.cascading_ext.workflow2.counter.CounterFilter;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public final class WorkflowRunner extends BaseWorkflowRunner<WorkflowRunner.ExecuteConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowRunner.class);


  public  WorkflowRunner(Class klass, Step tail) throws IOException {
    this(klass, new DbPersistenceFactory(), tail);
  }

  public  WorkflowRunner(Class klass, WorkflowOptions options, Step tail) throws IOException {
    this(klass, new DbPersistenceFactory(), options, tail);
  }

  public  WorkflowRunner(Class klass, Set<Step> tailSteps) throws IOException {
    this(klass, new DbPersistenceFactory(), tailSteps);
  }

  public WorkflowRunner(Class klass, WorkflowOptions options, Set<? extends BaseStep<ExecuteConfig>> tailSteps) throws IOException {
    this(klass, new DbPersistenceFactory(), options, tailSteps);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public  WorkflowRunner(WorkflowOptions options, Step tail) throws IOException {
    this(new DbPersistenceFactory(), options, tail);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public  WorkflowRunner(WorkflowOptions options, Set<Step> tailSteps) throws IOException {
    this(new DbPersistenceFactory(), options, tailSteps);
  }


  public  <K extends InitializedPersistence> WorkflowRunner(String workflowName, WorkflowPersistenceFactory<K, WorkflowOptions> persistence, WorkflowOptions options, Step tail) throws IOException {
    this(workflowName, persistence, options, Sets.newHashSet(tail));
  }

  public  <K extends InitializedPersistence> WorkflowRunner(Class klass, WorkflowPersistenceFactory<K, WorkflowOptions> persistence, Step tail) throws IOException {
    this(klass.getName(), persistence, tail);
  }

  public  <K extends InitializedPersistence> WorkflowRunner(Class klass, WorkflowPersistenceFactory<K, WorkflowOptions> persistence, WorkflowOptions options, final Step first, Step... rest) throws IOException {
    this(klass.getName(), persistence, options, combine(first, rest));
  }

  private static HashSet<Step> combine(final Step first, Step... rest) {
    HashSet<Step> s = new HashSet<>(Arrays.asList(rest));
    s.add(first);
    return s;
  }

  public  <K extends InitializedPersistence> WorkflowRunner(Class klass, WorkflowPersistenceFactory<K, WorkflowOptions> persistence, WorkflowOptions options, Set<? extends BaseStep<ExecuteConfig>> tailSteps) throws IOException {
    this(klass.getName(), persistence, options, tailSteps);
  }

  public  <K extends InitializedPersistence> WorkflowRunner(Class klass, WorkflowPersistenceFactory<K, WorkflowOptions> persistence, Set<Step> tailSteps) throws IOException {
    this(klass, persistence, new ProductionWorkflowOptions(), tailSteps);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public <K extends InitializedPersistence> WorkflowRunner(WorkflowPersistenceFactory<K, WorkflowOptions> persistence, WorkflowOptions options, Set<? extends BaseStep<ExecuteConfig>> tailSteps) throws IOException {
    this(persistence.initialize(options), tailSteps);
  }

  // This constructor requires that the given options contain an AppType for generating the workflow name
  public <K extends InitializedPersistence> WorkflowRunner(WorkflowPersistenceFactory<K, WorkflowOptions> persistence, WorkflowOptions options, Step tail) throws IOException {
    this(persistence, options, Sets.newHashSet(tail));
  }

  public <K extends InitializedPersistence> WorkflowRunner(String workflowName, WorkflowPersistenceFactory<K, WorkflowOptions> persistence, Step tail) throws IOException {
    this(workflowName, persistence, Sets.newHashSet(tail));
  }

  public <K extends InitializedPersistence> WorkflowRunner(String workflowName, WorkflowPersistenceFactory<K, WorkflowOptions> persistence, Set<? extends BaseStep<ExecuteConfig>> tail) throws IOException {
    this(workflowName, persistence, new ProductionWorkflowOptions(), Sets.newHashSet(tail));
  }

  public  <K extends InitializedPersistence> WorkflowRunner(String workflowName, WorkflowPersistenceFactory<K, WorkflowOptions> persistenceFactory, WorkflowOptions options, Set<? extends BaseStep<ExecuteConfig>> tailSteps) throws IOException {
    this(persistenceFactory.initialize(workflowName, options), tailSteps);
  }

  public  WorkflowRunner(InitializedWorkflow initializedData, Step tail) throws IOException {
    this(initializedData, Sets.newHashSet(tail));
  }

  public  <K extends InitializedPersistence> WorkflowRunner(InitializedWorkflow<K, WorkflowOptions> initializedData, Set<? extends BaseStep<ExecuteConfig>> tailSteps) throws IOException {

    super(initializedData, tailSteps,
        new ExecuteConfig(
            initializedData.getOptions().getLockProvider().create(),
            initializedData.getOptions().getCounterFilter()
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
    private transient CounterFilter counterFilter;

    public ExecuteConfig(StoreReaderLocker lockProvider, CounterFilter counterFilter) {
      this.lockProvider = lockProvider;
      this.counterFilter = counterFilter;
    }

    public StoreReaderLocker getLockProvider() {
      return lockProvider;
    }

    public CounterFilter getCounterFilter() {
      return counterFilter;
    }

  }

}
