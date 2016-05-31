package com.rapleaf.cascading_ext.workflow2.test;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.log4j.Level;

import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.cascading_ext.resource.ResourceManagers;
import com.liveramp.workflow_state.InitializedDbPersistence;
import com.rapleaf.cascading_ext.test.HadoopCommonJunit4TestCase;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.InMemoryContext;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnable;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.db_schemas.DatabasesImpl;

public class BaseWorkflowTestCase extends HadoopCommonJunit4TestCase {
  private static final String TEST_WORKFLOW_NAME = "Test Workflow";

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

  public WorkflowRunner execute(Action action) throws IOException {
    return execute(Sets.newHashSet(new Step(action)));
  }

  public WorkflowRunner execute(Action action, ResourceDeclarer resourceManager) throws IOException {
    return execute(Sets.newHashSet(new Step(action)), resourceManager);
  }

  public WorkflowRunner execute(Set<Step> steps) throws IOException {
    return execute(steps, context);
  }

  public WorkflowRunner execute(Set<Step> steps, WorkflowOptions options) throws IOException {
    return execute(steps, options, context);
  }

  public WorkflowRunner execute(Action action, WorkflowOptions options) throws IOException {
    return execute(Sets.newHashSet(new Step(action)), options, context);
  }

  public WorkflowRunner execute(Step step, WorkflowOptions options) throws IOException {
    return execute(Sets.newHashSet(step), options, context);
  }

  public WorkflowRunner execute(Set<Step> steps, ContextStorage storage) throws IOException {
    return execute(steps, new TestWorkflowOptions(), storage);
  }

  public WorkflowRunner execute(Set<Step> steps, WorkflowOptions options, ContextStorage storage) throws IOException {
    WorkflowRunner workflowRunner = new WorkflowRunner(TEST_WORKFLOW_NAME,
        new DbPersistenceFactory(),
        options
            .setStorage(storage)
            .setResourceManager(ResourceManagers.inMemoryResourceManager(TEST_WORKFLOW_NAME, null, new DatabasesImpl().getRlDb())),
        steps);
    workflowRunner.run();
    return workflowRunner;
  }

  public WorkflowRunner execute(InitializedWorkflow workflow, Action tail) throws IOException {
    return execute(workflow, Sets.<Step>newHashSet(new Step(tail)));
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
        new TestWorkflowOptions()
            .setResourceManager(resourceManager),
        steps);
    workflowRunner.run();
    return workflowRunner;
  }

  public InitializedWorkflow<InitializedDbPersistence> initializeWorkflow() throws IOException {
    return new DbPersistenceFactory().initialize(
        TEST_WORKFLOW_NAME,
        new TestWorkflowOptions()
            .setResourceManager(ResourceManagers.dbResourceManager(TEST_WORKFLOW_NAME, null, new DatabasesImpl().getRlDb())));
  }

  public void executeWorkflowFOff(WorkflowRunnable foff) throws Exception {
    WorkflowRunner runner = execute(foff.getSteps(), foff.getOptions());
    foff.postWorkflow(runner.getPersistence().getFlatCounters(), runner.getPersistence().getCountersByStep());
  }

  public InMemoryContext context() {
    return context;
  }

}
