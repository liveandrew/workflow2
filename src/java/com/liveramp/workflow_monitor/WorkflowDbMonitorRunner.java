package com.liveramp.workflow_monitor;

import java.util.Arrays;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.java_support.logging.LoggingHelper;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlerter;
import com.liveramp.workflow_monitor.alerts.execution.MapreduceJobAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.alerts.CPUUsage;
import com.liveramp.workflow_monitor.alerts.execution.alerts.DiedUnclean;
import com.liveramp.workflow_monitor.alerts.execution.alerts.GCTime;
import com.liveramp.workflow_monitor.alerts.execution.alerts.KilledTasks;
import com.liveramp.workflow_monitor.alerts.execution.alerts.OutputPerMapTask;
import com.liveramp.workflow_monitor.alerts.execution.alerts.ShortMaps;
import com.liveramp.workflow_monitor.alerts.execution.alerts.ShortReduces;
import com.liveramp.workflow_monitor.alerts.execution.recipient.FromPersistenceGenerator;
import com.liveramp.workflow_monitor.alerts.execution.recipient.TestRecipientGenerator;

public class WorkflowDbMonitorRunner {

  public static void main(String[] args) throws InterruptedException {
    LoggingHelper.setLoggingProperties(WorkflowDbMonitorRunner.class.getSimpleName());

    IDatabases db = new DatabasesImpl();
    db.getWorkflowDb().disableCaching();

    ExecutionAlerter production = new ExecutionAlerter(
        new FromPersistenceGenerator(db),
        Lists.<ExecutionAlertGenerator>newArrayList(
            new DiedUnclean()
        ),
        Lists.<MapreduceJobAlertGenerator>newArrayList(
            new KilledTasks(),
            new GCTime(),
            //            new NearMemoryLimit(),
            new CPUUsage(),
            new OutputPerMapTask()
        ),
        db
    );

    Set<String> testEmailVictims = Sets.newHashSet(Arrays.asList(
        "bpodgursky+alert-firehose@liveramp.com",
        "kong+alert-firehose@liveramp.com"));

    ExecutionAlerter testing = new ExecutionAlerter(
        new TestRecipientGenerator(
            AlertsHandlers.builder(TeamList.DEV_TOOLS)
                .setEngineeringRecipient(AlertRecipients.of(testEmailVictims))
                .build()),
        Lists.<ExecutionAlertGenerator>newArrayList(
        ),
        Lists.<MapreduceJobAlertGenerator>newArrayList(
            new ShortMaps(),
            new ShortReduces()
        ),
        db
    );


    WorkflowMonitor monitor = new WorkflowMonitor(
        Lists.newArrayList(
            production,
            testing
        )
    );

    monitor.monitor();

  }

}
