package com.rapleaf.cascading_ext.workflow2.test;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.log4j.Level;

import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceManagers;
import com.liveramp.workflow_core.ContextStorage;
import com.liveramp.workflow_core.InMemoryContext;
import com.liveramp.workflow_core.runner.BaseAction;
import com.liveramp.workflow_core.runner.BaseStep;
import com.liveramp.workflow_state.InitializedDbPersistence;
import com.rapleaf.cascading_ext.test.HadoopCommonJunit4TestCase;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnable;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.db_schemas.DatabasesImpl;

public class BaseWorkflowTestCase extends HadoopCommonJunit4TestCase {
  private static final String TEST_WORKFLOW_NAME = "Test workflow";

  private final InMemoryContext context;

  public BaseWorkflowTestCase(String projectName) {
    this(Level.ALL, projectName);
  }

  public BaseWorkflowTestCase(Level level, String projectName) {
    super(level, projectName);

    this.context = new InMemoryContext();
  }

  /*
   * Useful generic test cases.
   */
  public WorkflowRunner execute(Step step) throws IOException {
    return execute(Sets.newHashSet(step));
  }

  public WorkflowRunner execute(Step step, ResourceDeclarer resourceManager) throws IOException {
    return execute(Sets.newHashSet(step), resourceManager);
  }

  public WorkflowRunner execute(BaseAction action) throws IOException {
    return execute(Sets.newHashSet(new Step(action)));
  }

  public WorkflowRunner execute(BaseAction action, ResourceDeclarer resourceManager) throws IOException {
    return execute(Sets.newHashSet(new Step(action)), resourceManager);
  }

  public WorkflowRunner execute(Set<? extends BaseStep<WorkflowRunner.ExecuteConfig>> steps) throws IOException {
    return execute(steps, context);
  }

  public WorkflowRunner execute(Set<? extends BaseStep<WorkflowRunner.ExecuteConfig>> steps, WorkflowOptions options) throws IOException {
    return execute(steps, options, context);
  }

  public WorkflowRunner execute(BaseAction action, WorkflowOptions options) throws IOException {
    return execute(Sets.newHashSet(new Step(action)), options, context);
  }

  public WorkflowRunner execute(Step step, WorkflowOptions options) throws IOException {
    return execute(Sets.newHashSet(step), options, context);
  }

  public WorkflowRunner execute(Set<? extends BaseStep<WorkflowRunner.ExecuteConfig>> steps, ContextStorage storage) throws IOException {
    return execute(steps, WorkflowOptions.test(), storage);
  }

  public WorkflowRunner execute(Set<? extends BaseStep<WorkflowRunner.ExecuteConfig>> steps, WorkflowOptions options, ContextStorage storage) throws IOException {
    WorkflowRunner workflowRunner = new WorkflowRunner(TEST_WORKFLOW_NAME,
        new DbPersistenceFactory(),
        options
            .setStorage(storage)
            .setResourceManager(ResourceManagers.inMemoryResourceManager()),
        steps);
    workflowRunner.run();
    return workflowRunner;
  }

  public WorkflowRunner execute(InitializedWorkflow workflow, BaseAction tail) throws IOException {
    return execute(workflow, Sets.<Step>newHashSet(new Step(tail)));
  }

  public WorkflowRunner execute(InitializedWorkflow workflow, Step tail) throws IOException {
    return execute(workflow, Sets.<Step>newHashSet(tail));
  }

  public WorkflowRunner execute(InitializedWorkflow workflow, Set<Step> tails) throws IOException {
    WorkflowRunner workflowRunner = new WorkflowRunner(
        workflow,
        tails
    );
    workflowRunner.run();
    return workflowRunner;
  }

  public WorkflowRunner execute(Set<Step> steps, ResourceDeclarer resourceManager) throws IOException {
    WorkflowRunner workflowRunner = new WorkflowRunner(TEST_WORKFLOW_NAME,
        new DbPersistenceFactory(),
        WorkflowOptions.test()
            .setResourceManager(resourceManager),
        steps);
    workflowRunner.run();
    return workflowRunner;
  }

  public InitializedWorkflow<InitializedDbPersistence, WorkflowOptions> initializeWorkflow() throws IOException {
    return initializeWorkflow(TEST_WORKFLOW_NAME, ResourceManagers.dbResourceManager(TEST_WORKFLOW_NAME, null, new DatabasesImpl().getRlDb()));
  }

  public InitializedWorkflow<InitializedDbPersistence, WorkflowOptions> initializeWorkflow(String workflowName,
                                                                                           ResourceDeclarer declarer) throws IOException {
    return new DbPersistenceFactory().initialize(
        workflowName,
        WorkflowOptions.test()
            .setResourceManager(declarer));

  }


  public void executeWorkflowFOff(WorkflowRunnable foff) throws Exception {
    WorkflowRunner runner = execute(foff.getSteps(), foff.getOptions());
    foff.postWorkflow(runner.getPersistence().getFlatCounters(), runner.getPersistence().getCountersByStep());
  }

  public InMemoryContext context() {
    return context;
  }

}
