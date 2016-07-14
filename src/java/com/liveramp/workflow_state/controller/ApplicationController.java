package com.liveramp.workflow_state.controller;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import com.liveramp.commons.Accessors;
import com.liveramp.importer.generated.AppType;
import com.liveramp.workflow.types.WorkflowExecutionStatus;
import com.liveramp.workflow_state.ProcessStatus;
import com.liveramp.workflow_state.WorkflowQueries;
import com.liveramp.workflow_state.WorkflowRunnerNotification;
import com.rapleaf.db_schemas.IDatabases;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.Application;
import com.rapleaf.db_schemas.rldb.models.ApplicationConfiguredNotification;
import com.rapleaf.db_schemas.rldb.models.ConfiguredNotification;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;

//  TODO not liking all the staticness of this.  figure out later
public class ApplicationController {

  public static void cancelLatestExecution(IRlDb rldb, String workflowName, String scopeIdentifier) throws IOException {
    ExecutionController.cancelExecution(rldb, WorkflowQueries.getLatestExecution(rldb, workflowName, scopeIdentifier));
  }

  public static void cancelLatestExecution(IRlDb rlDb, AppType appType, String scopeIdentifier) throws IOException {
    Optional<WorkflowExecution> latestExecution = WorkflowQueries.getLatestExecution(rlDb, appType, scopeIdentifier);
    if (latestExecution.isPresent()) {
      ExecutionController.cancelExecution(rlDb, latestExecution.get());
    }
  }

  public static void cancelIfIncompleteExecution(IRlDb rlDb, AppType appType, String scopeIdentifier) throws IOException {
    Optional<WorkflowExecution> latestExecution = WorkflowQueries.getLatestExecution(rlDb, appType, scopeIdentifier);
    if (isIncomplete(latestExecution)) {
      ExecutionController.cancelExecution(rlDb, latestExecution.get());
    }
  }

  public static boolean isRunning(IRlDb rlDb, AppType appType, String scopeIdentifier) throws IOException {
    Optional<WorkflowExecution> latestExecution = WorkflowQueries.getLatestExecution(rlDb, appType, scopeIdentifier);
    if (latestExecution.isPresent()) {
      return ExecutionController.isRunning(latestExecution.get());
    } else {
      return false;
    }
  }

  public static boolean isLatestExecutionIncomplete(IRlDb rlDb, AppType appType, String scopeIdentifier) throws IOException {
    return isIncomplete(WorkflowQueries.getLatestExecution(rlDb, appType, scopeIdentifier));
  }

  public static int numRunningInstances(IDatabases db, AppType appType) throws  IOException {
    Multimap<WorkflowExecution, WorkflowAttempt> incomplete = WorkflowQueries.getExecutionsToAttempts(db, appType, WorkflowExecutionStatus.INCOMPLETE);

    int runningInstances = 0;
    for (Map.Entry<WorkflowExecution, WorkflowAttempt> entry : incomplete.entries()) {
      if (WorkflowQueries.getProcessStatus(entry.getValue(), entry.getKey()) == ProcessStatus.ALIVE) {
        runningInstances++;
      }
    }

    return runningInstances;
  }


  public static void addConfiguredNotifications(IRlDb rlDb, String workflowName, String email, Set<WorkflowRunnerNotification> notifications) throws IOException {
    Application application = Accessors.only(rlDb.applications().findByName(workflowName));

    Set<WorkflowRunnerNotification> existing = Sets.newHashSet();
    for (ConfiguredNotification.Attributes attributes : WorkflowQueries.getApplicationNotifications(rlDb, application.getId(), email)) {
      existing.add(WorkflowRunnerNotification.findByValue(attributes.getWorkflowRunnerNotification()));
    }

    for (WorkflowRunnerNotification notification : notifications) {
      if (!existing.contains(notification)) {
        ConfiguredNotification configured = rlDb.configuredNotifications().create(notification.ordinal(), email, false);
        rlDb.applicationConfiguredNotifications().create(application.getId(), configured.getId());
      }
    }

  }

  public static void removeConfiguredNotifications(IRlDb rlDb, String workflowName, String email) throws IOException {
    removeConfiguredNotifications(rlDb, workflowName, email, EnumSet.allOf(WorkflowRunnerNotification.class));
  }

  public static void removeConfiguredNotifications(IRlDb rlDb, String workflowName, String email, Set<WorkflowRunnerNotification> notificaions) throws IOException {
    Application application = Accessors.only(rlDb.applications().findByName(workflowName));
    long appId = application.getId();

    for (ConfiguredNotification.Attributes attributes : WorkflowQueries.getApplicationNotifications(rlDb, appId, email)) {
      if (notificaions.contains(WorkflowRunnerNotification.findByValue(attributes.getWorkflowRunnerNotification()))) {

        for (ApplicationConfiguredNotification appNotification : rlDb.applicationConfiguredNotifications().query()
            .applicationId(appId)
            .configuredNotificationId(attributes.getId())
            .find()) {

          rlDb.applicationConfiguredNotifications().delete(appNotification);
          //  TODO if a ConfiguredNotification has no App/Execution/Attempt ConfiguredNotifications referencing it, delete it
        }
      }
    }

  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static boolean isIncomplete(Optional<WorkflowExecution> execution){
    return execution.isPresent() && execution.get().getStatus() == WorkflowExecutionStatus.INCOMPLETE.ordinal();
  }

}
