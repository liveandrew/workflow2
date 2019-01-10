
/**
 * Autogenerated by Jack
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.liveramp.databases.workflow_db.iface;

import com.liveramp.databases.workflow_db.models.WorkflowExecutionConfiguredNotification;
import com.liveramp.databases.workflow_db.query.WorkflowExecutionConfiguredNotificationQueryBuilder;
import com.liveramp.databases.workflow_db.query.WorkflowExecutionConfiguredNotificationDeleteBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.List;

import com.rapleaf.jack.IModelPersistence;

public interface IWorkflowExecutionConfiguredNotificationPersistence extends IModelPersistence<WorkflowExecutionConfiguredNotification> {
  WorkflowExecutionConfiguredNotification create(final long workflow_execution_id, final long configured_notification_id) throws IOException;

  WorkflowExecutionConfiguredNotification createDefaultInstance() throws IOException;
  List<WorkflowExecutionConfiguredNotification> findByWorkflowExecutionId(long value)  throws IOException;
  List<WorkflowExecutionConfiguredNotification> findByConfiguredNotificationId(long value)  throws IOException;

  WorkflowExecutionConfiguredNotificationQueryBuilder query();

  WorkflowExecutionConfiguredNotificationDeleteBuilder delete();
}