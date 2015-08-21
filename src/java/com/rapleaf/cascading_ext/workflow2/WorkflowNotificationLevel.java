package com.rapleaf.cascading_ext.workflow2;

/**

 DEBUG -> start
 INFO -> finish
 WARN -> shutdown
 ERROR -> fail, died unclean

 */

public enum WorkflowNotificationLevel {
  DEBUG,
  INFO ,
  WARN,
  ERROR,
  NONE
}
