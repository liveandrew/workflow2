package com.liveramp.workflow2.workflow_examples.background;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;

import com.liveramp.workflow2.workflow_examples.AlertsHandlerWorkflow;
import com.liveramp.workflow_core.CoreOptions;
import com.liveramp.workflow_core.background_workflow.AlwaysStart;
import com.liveramp.workflow_core.background_workflow.BackgroundAction;
import com.liveramp.workflow_core.background_workflow.BackgroundStep;
import com.liveramp.workflow_core.background_workflow.BackgroundWorkflowPreparer;
import com.liveramp.workflow_core.background_workflow.BackgroundWorkflowSubmitter;
import com.liveramp.workflow_core.background_workflow.MultiStep;
import com.liveramp.workflow_core.background_workflow.PreconditionFunction;
import com.liveramp.workflow_db_state.background_workflow.BackgroundPersistenceFactory;
import com.liveramp.workflow_db_state.background_workflow.BackgroundWorkflow;
import com.rapleaf.cascading_ext.workflow2.state.InitializedWorkflow;

public class SimpleBackgroundWorkflow {

  public static void main(String[] args) throws IOException {
    String scope = args[0];

    BackgroundWorkflow initialized = BackgroundWorkflowPreparer.initialize(
        SimpleBackgroundWorkflow.class.getName(),
        new BackgroundPersistenceFactory(),
        CoreOptions.test().setScope(scope)
    );

    InitializedWorkflow.ScopedContext rootScope = initialized.getRootScope();

    SimpleWorkflowContext context = new SimpleWorkflowContext(scope);

    BackgroundStep action1 = new BackgroundStep(
        rootScope,
        "action1",
        TestAction.class,
        context
    );

    BackgroundStep action2 = new MultiStep<>(
        rootScope,
        "action2",
        context,
        new SimpleAssembly(context),
        Sets.newHashSet(action1)
    );

    BackgroundStep action3 = new BackgroundStep(
        rootScope,
        "action3",
        BlockUntil.class,
        DateTime.now().plusMinutes(30).getMillis(),
        Duration.ofMinutes(5),
        Sets.newHashSet(action2)
    );

    BackgroundWorkflowSubmitter.submit(
        initialized,
        Lists.newArrayList(action3)
    );

  }

  public static class SimpleWorkflowContext implements Serializable {
    private final String message;

    public SimpleWorkflowContext(String message) {
      this.message = message;
    }
  }

  public static class BlockUntil extends BackgroundAction<Long> {
    private Long context;

    public static class WaitUntil implements PreconditionFunction<Long> {
      @Override
      public Boolean apply(Long context) {
        return System.currentTimeMillis() > context;
      }
    }

    public BlockUntil() {
      super(new WaitUntil());
    }

    @Override
    public void initializeInternal(Long data) {
      this.context = data;
    }

    @Override
    protected void execute() throws Exception {
      setStatusMessage("Waited until after " + context + " to run!");
    }
  }

  public static class TestAction extends BackgroundAction<SimpleWorkflowContext> {

    private String variable;

    public TestAction() {
      super(new AlwaysStart<>());
    }

    @Override
    public void initializeInternal(SimpleWorkflowContext testWorkflowContext) {
      this.variable = testWorkflowContext.message;
    }

    @Override
    protected void execute() throws Exception {
      setStatusMessage(variable);
    }

  }

  public static class NoOp extends BackgroundAction<SimpleWorkflowContext> {

    public NoOp() {
      super(new AlwaysStart<>());
    }

    @Override
    public void initializeInternal(SimpleWorkflowContext testWorkflowContext) {
    }

    @Override
    protected void execute() throws Exception {
    }

  }

  public static class SimpleAssembly implements MultiStep.StepAssembly<Void, SimpleWorkflowContext> {

    private final SimpleWorkflowContext sharedContext;

    SimpleAssembly(SimpleWorkflowContext sharedContext) {
      this.sharedContext = sharedContext;
    }

    @Override
    public Set<BackgroundStep> constructTails(InitializedWorkflow.ScopedContext context) {

      BackgroundStep step = new BackgroundStep(
          context,
          "sub1",
          NoOp.class,
          sharedContext
      );

      BackgroundStep step2 = new BackgroundStep(
          context,
          "sub2",
          NoOp.class,
          sharedContext,
          Duration.ZERO,
          Sets.newHashSet(step)
      );

      return Sets.newHashSet(step2);
    }
  }


}