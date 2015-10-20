package com.rapleaf.cascading_ext.workflow2;

import java.util.EnumSet;

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

  public static final EnumSet<WorkflowRunnerNotification> DEBUG = EnumSet.of(
      WorkflowRunnerNotification.START,
      WorkflowRunnerNotification.SUCCESS,
      WorkflowRunnerNotification.SHUTDOWN,
      WorkflowRunnerNotification.FAILURE
  );

  public static final EnumSet<WorkflowRunnerNotification> INFO = EnumSet.of(
      WorkflowRunnerNotification.SUCCESS,
      WorkflowRunnerNotification.SHUTDOWN,
      WorkflowRunnerNotification.FAILURE
  );

  public static final EnumSet<WorkflowRunnerNotification> WARN = EnumSet.of(
      WorkflowRunnerNotification.SHUTDOWN,
      WorkflowRunnerNotification.FAILURE
  );

  public static final EnumSet<WorkflowRunnerNotification> ERROR = EnumSet.of(
      WorkflowRunnerNotification.FAILURE
  );

  public static final EnumSet<WorkflowRunnerNotification> NONE = EnumSet.noneOf(WorkflowRunnerNotification.class);

}
