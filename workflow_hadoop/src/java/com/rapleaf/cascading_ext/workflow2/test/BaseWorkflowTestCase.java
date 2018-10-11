package com.rapleaf.cascading_ext.workflow2.test;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import com.sleepycat.je.dbi.DatabaseImpl;
import org.apache.log4j.Level;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;

import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.workflow.state.DbHadoopWorkflow;
import com.liveramp.workflow.state.WorkflowDbPersistenceFactory;
import com.liveramp.workflow2.workflow_hadoop.ResourceManagers;
import com.liveramp.workflow_core.ContextStorage;
import com.liveramp.workflow_core.InMemoryContext;
import com.liveramp.workflow_core.runner.BaseAction;
import com.rapleaf.cascading_ext.test.HadoopCommonJunit4TestCase;
import com.rapleaf.cascading_ext.workflow2.FailingAction;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunnable;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.DatabasesImpl;


import static org.junit.Assert.fail;

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

  @Before
  public void deleteWorkflowDbFixtures() throws IOException {
    new com.liveramp.databases.workflow_db.DatabasesImpl().getWorkflowDb().deleteAll();

    //  TODO sweep as soon as old db resource managers aren't used in tests
    IRlDb rldb = new DatabasesImpl().getRlDb();
    rldb.resourceRecords().deleteAll();
    rldb.resourceRoots().deleteAll();
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

  public WorkflowRunner execute(Set<Step> steps) throws IOException {
    return execute(steps, context);
  }

  public WorkflowRunner execute(Set<Step> steps, WorkflowOptions options) throws IOException {
    return execute(steps, options, context, ResourceManagers.inMemoryResourceManager());
  }

  public WorkflowRunner execute(BaseAction action, WorkflowOptions options) throws IOException {
    return execute(Sets.newHashSet(new Step(action)), options, context, ResourceManagers.inMemoryResourceManager());
  }

  public WorkflowRunner execute(Step step, WorkflowOptions options) throws IOException {
    return execute(Sets.newHashSet(step), options, context, ResourceManagers.inMemoryResourceManager());
  }

  public WorkflowRunner execute(Set<Step> steps, ContextStorage storage) throws IOException {
    return execute(steps, WorkflowOptions.test(), storage, ResourceManagers.inMemoryResourceManager());
  }

  public WorkflowRunner execute(Set<Step> steps,
                                WorkflowOptions options,
                                ContextStorage storage,
                                ResourceDeclarer manager) throws IOException {
    WorkflowRunner workflowRunner = new WorkflowRunner(TEST_WORKFLOW_NAME,
        new WorkflowDbPersistenceFactory(),
        options
            .setStorage(storage)
            .setResourceManager(manager),
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

  public WorkflowRunner executeAndRollback(Set<Step> steps) throws IOException {
    return executeAndRollback(steps, ResourceManagers.inMemoryResourceManager());
  }

  public WorkflowRunner executeAndRollback(Set<Step> steps, ResourceDeclarer declarer) throws IOException {

    Step terminalFail = new Step(new FailingAction("terminal-fail"), steps);

    WorkflowRunner runner = buildWorkflowRunner(Sets.newHashSet(terminalFail),
        WorkflowOptions.test().setRollBackOnFailure(true),
        declarer
    );

    try {
      runner.run();
      fail();
    } catch (Exception e) {
      //  expected
    }

    return runner;
  }

  public WorkflowRunner execute(Set<Step> steps, ResourceDeclarer resourceManager) throws IOException {
    WorkflowRunner workflowRunner = buildWorkflowRunner(steps, WorkflowOptions.test(), resourceManager);
    workflowRunner.run();
    return workflowRunner;
  }

  @NotNull
  private WorkflowRunner buildWorkflowRunner(Set<Step> steps,
                                             WorkflowOptions options,
                                             ResourceDeclarer resourceManager) throws IOException {
    return new WorkflowRunner(TEST_WORKFLOW_NAME,
        new WorkflowDbPersistenceFactory(),
        options
            .setResourceManager(resourceManager),
        steps
    );
  }

  public DbHadoopWorkflow initializeWorkflow() throws IOException {
    return initializeWorkflow(TEST_WORKFLOW_NAME, ResourceManagers.dbResourceManager(TEST_WORKFLOW_NAME, null, new com.liveramp.databases.workflow_db.DatabasesImpl().getWorkflowDb()));
  }

  public DbHadoopWorkflow initializeWorkflow(String workflowName,
                                             ResourceDeclarer declarer) throws IOException {
    return new WorkflowDbPersistenceFactory().initialize(
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
