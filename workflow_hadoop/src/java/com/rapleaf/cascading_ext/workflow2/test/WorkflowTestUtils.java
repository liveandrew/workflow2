package com.rapleaf.cascading_ext.workflow2.test;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import com.liveramp.cascading_ext.resource.ResourceDeclarer;
import com.liveramp.workflow.state.WorkflowDbPersistenceFactory;
import com.liveramp.workflow2.workflow_hadoop.ResourceManagers;
import com.liveramp.workflow_core.ContextStorage;
import com.liveramp.workflow_core.InMemoryContext;
import com.liveramp.workflow_core.runner.BaseAction;
import com.rapleaf.cascading_ext.workflow2.FailingAction;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.WorkflowRunner;
import com.rapleaf.cascading_ext.workflow2.options.HadoopWorkflowOptions;

public class WorkflowTestUtils {

  public static WorkflowRunner execute(Step step) throws IOException {
    return execute(Sets.newHashSet(step));
  }

  public static WorkflowRunner execute(Set<Step> steps) throws IOException {
    return execute(steps, new InMemoryContext());
  }

  public static WorkflowRunner execute(Set<Step> steps, HadoopWorkflowOptions options) throws IOException {
    return execute(steps, options, new InMemoryContext(), ResourceManagers.inMemoryResourceManager());
  }

  public static WorkflowRunner execute(BaseAction action, HadoopWorkflowOptions options) throws IOException {
    return execute(Sets.newHashSet(new Step(action)), options, new InMemoryContext(), ResourceManagers.inMemoryResourceManager());
  }

  public static WorkflowRunner execute(Step step, HadoopWorkflowOptions options) throws IOException {
    return execute(Sets.newHashSet(step), options, new InMemoryContext(), ResourceManagers.inMemoryResourceManager());
  }

  public static WorkflowRunner execute(Set<Step> steps, ContextStorage storage) throws IOException {
    return execute(steps, HadoopWorkflowOptions.test(), storage, ResourceManagers.inMemoryResourceManager());
  }

  public static WorkflowRunner execute(Set<Step> steps,
                                HadoopWorkflowOptions options,
                                ContextStorage storage,
                                ResourceDeclarer manager) throws IOException {
    WorkflowRunner workflowRunner = new WorkflowRunner(WorkflowTestUtils.class.getName(),
        new WorkflowDbPersistenceFactory(),
        options
            .setStorage(storage)
            .setResourceManager(manager),
        steps);
    workflowRunner.run();
    return workflowRunner;
  }

  @NotNull
  private static WorkflowRunner buildWorkflowRunner(Set<Step> steps,
                                             HadoopWorkflowOptions options,
                                             ResourceDeclarer resourceManager) throws IOException {
    return new WorkflowRunner(WorkflowTestUtils.class.getName(),
        new WorkflowDbPersistenceFactory(),
        options
            .setResourceManager(resourceManager),
        steps
    );
  }

  public static WorkflowRunner executeAndRollback(Set<Step> steps, ResourceDeclarer declarer) throws IOException {

    Step terminalFail = new Step(new FailingAction("terminal-fail"), steps);

    WorkflowRunner runner = buildWorkflowRunner(Sets.newHashSet(terminalFail),
        HadoopWorkflowOptions.test().setRollBackOnFailure(true),
        declarer
    );

    try {
      runner.run();
      Assert.fail();
    } catch (Exception e) {
      //  expected
    }

    return runner;
  }


}
