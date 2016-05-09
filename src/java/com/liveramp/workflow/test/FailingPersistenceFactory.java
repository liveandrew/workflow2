package com.liveramp.workflow.test;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.liveramp.importer.generated.AppType;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.functional.Fn;
import com.liveramp.java_support.functional.Fns;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.liveramp.workflow_state.WorkflowStatePersistence;
import com.rapleaf.cascading_ext.workflow2.Step;
import com.rapleaf.cascading_ext.workflow2.state.WorkflowPersistenceFactory;

public class FailingPersistenceFactory implements WorkflowPersistenceFactory {

  protected final WorkflowPersistenceFactory delegate;
  private final Set<String> stepsToFailFullNames;

  public FailingPersistenceFactory(WorkflowPersistenceFactory delegate, StepNameBuilder stepNameBuilder) {
    this(delegate, Sets.newHashSet(stepNameBuilder));
  }

  /**
   * @param stepNameBuilders any given steps are failed after they finish executing
   */
  public FailingPersistenceFactory(WorkflowPersistenceFactory delegate, Set<StepNameBuilder> stepNameBuilders) {
    this.delegate = delegate;
    this.stepsToFailFullNames = Sets.newHashSet(Fns.map(new Fn<StepNameBuilder, String>() {
      @Override
      public String apply(StepNameBuilder input) {
        return input.getCompositeStepName();
      }
    }, stepNameBuilders));
  }

  @Override
  public WorkflowStatePersistence prepare(DirectedGraph<Step, DefaultEdge> flatSteps,
                                          String name,
                                          String scopeId,
                                          String description,
                                          AppType appType,
                                          String host,
                                          String username,
                                          String pool,
                                          String priority,
                                          String launchDir,
                                          String launchJar,
                                          Set<WorkflowRunnerNotification> configuredNotifications,
                                          AlertsHandler configuredHandler,
                                          String remote,
                                          String implementationBuild) {
    return new FailingPersistence(delegate.prepare(flatSteps, name, scopeId, description, appType, host, username, pool, priority, launchDir, launchJar, configuredNotifications, configuredHandler, remote, implementationBuild), stepsToFailFullNames);
  }

  public static class IntentionallyFailedStepException extends RuntimeException {
    public IntentionallyFailedStepException(String message) {
      super(message);
    }
  }

  private static class FailingPersistence extends ForwardingPersistence {

    private final Set<String> stepsToFailFullNames;

    private FailingPersistence(WorkflowStatePersistence delegate, Set<String> stepsToFailFullNames) {
      super(delegate);
      this.stepsToFailFullNames = stepsToFailFullNames;
    }

    @Override
    public void markStepCompleted(String stepToken) throws IOException {
      delegatePersistence.markStepCompleted(stepToken);
      if (stepsToFailFullNames.contains(stepToken)) {
        IntentionallyFailedStepException exception = new IntentionallyFailedStepException(String.format("Failed step intentionally: %s", stepToken));
        delegatePersistence.markStepFailed(stepToken, exception);
        throw exception;
      }
    }

  }
}
