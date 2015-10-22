package com.rapleaf.cascading_ext.workflow2;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.Sets;

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

  public static final Set<WorkflowRunnerNotification> ERROR = new SetBuilder<>(Sets.newHashSet(NONE))
      .add(WorkflowRunnerNotification.FAILURE, WorkflowRunnerNotification.DIED_UNCLEAN, WorkflowRunnerNotification.INTERNAL_ERROR)
      .get();

  public static final Set<WorkflowRunnerNotification> WARN = new SetBuilder<>(Sets.newHashSet(ERROR))
      .add(WorkflowRunnerNotification.SHUTDOWN)
      .get();

  public static final Set<WorkflowRunnerNotification> INFO = new SetBuilder<>(Sets.newHashSet(WARN))
      .add(WorkflowRunnerNotification.SUCCESS)
      .get();

  public static final Set<WorkflowRunnerNotification> DEBUG = new SetBuilder<>(Sets.newHashSet(INFO))
      .add(WorkflowRunnerNotification.START)
      .get();

}
