package com.rapleaf.cascading_ext.workflow2;

import java.util.EnumSet;
import java.util.Set;

import com.liveramp.commons.collections.set.SetBuilder;
import com.liveramp.workflow_state.WorkflowRunnerNotification;

/**

 DEBUG -> start
 INFO -> finish
 WARN -> shutdown
 ERROR -> fail, died unclean

 */

public class WorkflowNotificationLevel {

  //  nope
  private WorkflowNotificationLevel() {
  }

  //  only alert on asynchronous errors generated by the workflow monitor (performance problems like GC, or unexpected process termination)
  public static final Set<WorkflowRunnerNotification> MONITOR = new SetBuilder<>(EnumSet.noneOf(WorkflowRunnerNotification.class))
      .add(WorkflowRunnerNotification.DIED_UNCLEAN)
      .add(WorkflowRunnerNotification.PERFORMANCE)
      .get();

  public static final Set<WorkflowRunnerNotification> ERROR = new SetBuilder<>(MONITOR)
      .add(WorkflowRunnerNotification.FAILURE, WorkflowRunnerNotification.INTERNAL_ERROR, WorkflowRunnerNotification.STEP_FAILURE)
      .get();

  public static final Set<WorkflowRunnerNotification> WARN = new SetBuilder<>(ERROR)
      .add(WorkflowRunnerNotification.SHUTDOWN)
      .get();

  public static final Set<WorkflowRunnerNotification> INFO = new SetBuilder<>(WARN)
      .add(WorkflowRunnerNotification.SUCCESS)
      .get();

  public static final Set<WorkflowRunnerNotification> DEBUG = new SetBuilder<>(INFO)
      .add(WorkflowRunnerNotification.START)
      .add(WorkflowRunnerNotification.STEP_SUCCESS)
      .get();

}
