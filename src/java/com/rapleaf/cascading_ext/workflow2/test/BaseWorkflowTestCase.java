package com.rapleaf.cascading_ext.workflow2.test;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.log4j.Level;

import com.liveramp.cascading_ext.resource.ResourceManager;
import com.liveramp.cascading_ext.resource.ResourceManagers;
import com.rapleaf.cascading_ext.test.HadoopCommonJunit4TestCase;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.cascading_ext.workflow2.ContextStorage;
import com.rapleaf.cascading_ext.workflow2.InMemoryContext;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.options.TestWorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.options.WorkflowOptions;
import com.rapleaf.cascading_ext.workflow2.state.DbPersistenceFactory;
import com.rapleaf.db_schemas.DatabasesImpl;

public class BaseWorkflowTestCase extends HadoopCommonJunit4TestCase {

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

  public WorkflowRunner execute(Step step, ResourceManager resourceManager) throws IOException {
    return execute(Sets.newHashSet(step), resourceManager);
  }

  public WorkflowRunner execute(Action action) throws IOException {
    return execute(Sets.newHashSet(new Step(action)));
  }

  public WorkflowRunner execute(Action action, ResourceManager resourceManager) throws IOException {
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
    WorkflowRunner workflowRunner = new WorkflowRunner("Test workflow",
        new DbPersistenceFactory(),
        options
            .setStorage(storage)
            .setResourceManager(ResourceManagers.inMemoryResourceManager("Test Workflow", null, new DatabasesImpl().getRlDb())),
        steps);
    workflowRunner.run();
    return workflowRunner;
  }

  public WorkflowRunner execute(Set<Step> steps, ResourceManager resourceManager) throws IOException {
    WorkflowRunner workflowRunner = new WorkflowRunner("Test workflow",
        new DbPersistenceFactory(),
        new TestWorkflowOptions()
            .setResourceManager(resourceManager),
        steps);
    workflowRunner.run();
    return workflowRunner;
  }

  public InMemoryContext context() {
    return context;
  }

}
