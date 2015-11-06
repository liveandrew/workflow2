package com.liveramp.workflow_monitor.alerts.execution;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import com.liveramp.commons.collections.map.MultimapBuilder;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.java_support.alerts_handler.InMemoryAlertsHandler;
import com.liveramp.workflow_monitor.alerts.execution.alert.AlertMessage;
import com.liveramp.workflow_monitor.alerts.execution.recipient.TestRecipientGenerator;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.rldb.IRlDb;
import com.rapleaf.db_schemas.rldb.models.Application;
import com.rapleaf.db_schemas.rldb.models.MapreduceJob;
import com.rapleaf.db_schemas.rldb.models.StepAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowAttempt;
import com.rapleaf.db_schemas.rldb.models.WorkflowExecution;
import com.rapleaf.db_schemas.rldb.workflow.StepStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowExecutionStatus;
import com.rapleaf.db_schemas.rldb.workflow.WorkflowRunnerNotification;

import static org.junit.Assert.assertEquals;

public class TestExecutionAlerter extends WorkflowMonitorTestCase {

  @Test
  public void testQueries() throws Exception {
    DatabasesImpl databases = new DatabasesImpl();
    IRlDb rldb = databases.getRlDb();
    databases.getRlDb().disableCaching();

    long currentTime = System.currentTimeMillis();

    Application application = rldb.applications().create("Test Workflow");

    WorkflowExecution execution = rldb.workflowExecutions().create("Test Workflow", WorkflowExecutionStatus.COMPLETE.ordinal())
        .setStartTime(currentTime - 2)
        .setEndTime(currentTime - 1)
        .setApplicationId(application.getIntId());
    execution.save();

    WorkflowAttempt attempt = rldb.workflowAttempts().create(execution.getIntId(), "", "", "", "");
    StepAttempt step = rldb.stepAttempts().create(attempt.getIntId(), "step", StepStatus.COMPLETED.ordinal(), "")
        .setEndTime(System.currentTimeMillis() - 1);
    step.save();
    MapreduceJob mapreduceJob = rldb.mapreduceJobs().create(step.getIntId(), "Job1", "JobName", "");
    rldb.mapreduceCounters().create(mapreduceJob.getIntId(), "Group", "Name", 1);

    InMemoryAlertsHandler handler = new InMemoryAlertsHandler();

    ExecutionAlerter alerter = new ExecutionAlerter(new TestRecipientGenerator(handler),
        Lists.<ExecutionAlertGenerator>newArrayList(new TestExecutionGenerator()),
        Lists.<MapreduceJobAlertGenerator>newArrayList(new TestJobGenerator()),
        databases
    );

    alerter.generateAlerts();

    List<String> alerts = handler.getAlerts();

    assertEquals(2, alerts.size());

    assertStringsContainSubstring("Alerting about job " + mapreduceJob.getId(), alerts);
    assertStringsContainSubstring("Alerting about execution " + execution.getId(), alerts);

  }


  private static class TestJobGenerator extends MapreduceJobAlertGenerator {
    protected TestJobGenerator() {
      super(new MultimapBuilder<String, String>().put("Group", "Name").get());
    }

    @Override
    public List<AlertMessage> generateAlerts(MapreduceJob job, TwoNestedMap<String, String, Long> counters) throws IOException {
      if (counters.get("Group", "Name") == 1) {
        return Lists.newArrayList(new AlertMessage("Alerting about job " + job.getId(), WorkflowRunnerNotification.PERFORMANCE));
      }
      return Lists.newArrayList();
    }
  }

  private static class TestExecutionGenerator implements ExecutionAlertGenerator {

    @Override
    public List<AlertMessage> generateAlerts(WorkflowExecution execution, Collection<WorkflowAttempt> attempts) throws IOException {
      return Lists.newArrayList(new AlertMessage("Alerting about execution " + execution.getId(), WorkflowRunnerNotification.PERFORMANCE));
    }
  }
}