package com.rapleaf.cascading_ext.workflow2;

import java.util.EnumSet;
import java.util.Set;

import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;
import com.rapleaf.support.collections.SetBuilder;

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

  public static final Set<WorkflowRunnerNotification> NONE = EnumSet.noneOf(WorkflowRunnerNotification.class);

  public static final Set<WorkflowRunnerNotification> ERROR = new SetBuilder<>(NONE)
      .add(WorkflowRunnerNotification.FAILURE, WorkflowRunnerNotification.DIED_UNCLEAN, WorkflowRunnerNotification.INTERNAL_ERROR, WorkflowRunnerNotification.STEP_FAILURE)
      .get();

  public static final Set<WorkflowRunnerNotification> WARN = new SetBuilder<>(ERROR)
      .add(WorkflowRunnerNotification.SHUTDOWN)
      .get();

  public static final Set<WorkflowRunnerNotification> INFO = new SetBuilder<>(WARN)
      .add(WorkflowRunnerNotification.SUCCESS)
      .get();

  public static final Set<WorkflowRunnerNotification> DEBUG = new SetBuilder<>(INFO)
      .add(WorkflowRunnerNotification.START)
      .get();

}
