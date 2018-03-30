package com.liveramp.workflow.helper;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.databases.workflow_db.models.StepAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.importer.generated.AppType;
import com.liveramp.workflow.types.StepStatus;
import com.liveramp.workflow.types.WorkflowExecutionStatus;
import com.rapleaf.jack.transaction.ITransactor;
import com.rapleaf.support.CommandLineHelper;

public class WorkflowExecutionHelper {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowExecutionHelper.class);

  /**
   * @return true if all job cancellation succeeds
   */
  public static boolean cancelRunningJobs(ITransactor<IWorkflowDb> workflowTransactor, AppType appType, String scopeIdentifier) {
    List<String> applicationIdentifiers = getRunningJobIdentifiers(workflowTransactor, appType, scopeIdentifier);
    if (applicationIdentifiers.isEmpty()) {
      LOG.info("No yarn application to cancel");
      return true;
    }

    boolean success = true;
    for (String applicationIdentifier : applicationIdentifiers) {
      try {
        LOG.info("Running yarn command to kill application {}", applicationIdentifier);
        cancelRunningJob(applicationIdentifier);
      } catch (Exception e) {
        LOG.error("Failed to kill application " + applicationIdentifier, e);
        success = false;
      }
    }

    return success;
  }

  /**
   * @return a list of job identifiers for running map reduce jobs in the form of "application_xxx_yyyy"
   */
  public static List<String> getRunningJobIdentifiers(ITransactor<IWorkflowDb> workflowTransactor, AppType appType, String scopeIdentifier) {
    return workflowTransactor.query(db -> {
      List<Long> executionIds = db.createQuery().from(WorkflowExecution.TBL)
          .where(WorkflowExecution.APP_TYPE.equalTo(appType.getValue()))
          .where(WorkflowExecution.NAME.equalTo(appType.name()))
          .where(WorkflowExecution.SCOPE_IDENTIFIER.equalTo(scopeIdentifier))
          .where(WorkflowExecution.STATUS.equalTo(WorkflowExecutionStatus.INCOMPLETE.getValue()))
          .select(WorkflowExecution.ID)
          .fetch()
          .gets(WorkflowExecution.ID);

      List<Long> attemptIds = db.createQuery().from(WorkflowAttempt.TBL)
          .where(WorkflowAttempt.WORKFLOW_EXECUTION_ID.as(Long.class).in(executionIds))
          .select(WorkflowAttempt.ID)
          .fetch()
          .gets(WorkflowAttempt.ID);

      List<Long> stepIds = db.createQuery().from(StepAttempt.TBL)
          .where(StepAttempt.WORKFLOW_ATTEMPT_ID.as(Long.class).in(attemptIds))
          .where(StepAttempt.STEP_STATUS.equalTo(StepStatus.RUNNING.getValue()))
          .select(StepAttempt.ID)
          .fetch()
          .gets(StepAttempt.ID);

      return db.createQuery().from(MapreduceJob.TBL)
          .where(MapreduceJob.STEP_ATTEMPT_ID.in(stepIds))
          .select(MapreduceJob.JOB_IDENTIFIER)
          .fetch()
          .gets(MapreduceJob.JOB_IDENTIFIER)
          .stream()
          .map(jobIdentifier -> {
            String[] tokens = jobIdentifier.split("_");
            return String.format("application_%s_%s", tokens[1], tokens[2]);
          })
          .collect(Collectors.toList());
    });
  }

  public static void cancelRunningJob(String applicationIdentifier) throws IOException {
    String output = CommandLineHelper.shell(String.format("yarn application -kill %s", applicationIdentifier), Duration.ofSeconds(10).toMillis());
    LOG.info(output);
  }

}
