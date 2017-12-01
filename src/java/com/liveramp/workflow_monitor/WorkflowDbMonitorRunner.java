package com.liveramp.workflow_monitor;

import java.util.Arrays;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.Level;

import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.liveramp.java_support.logging.LogOptions;
import com.liveramp.java_support.logging.LoggingHelper;
import com.liveramp.workflow_db_state.ThreadLocalWorkflowDb;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlerter;
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

    LoggingHelper.configureLoggers(LogOptions.name(WorkflowDbMonitorRunner.class.getSimpleName())
        .addLogstashCustom(TeamList.DEV_TOOLS, Level.INFO)
        .addDRFA()
    );

    ThreadLocal<IDatabases> db = new ThreadLocalWorkflowDb();

    ExecutionAlerter production = new ExecutionAlerter(
        new FromPersistenceGenerator(db.get()),
        Lists.newArrayList(
            new DiedUnclean()
        ),
        Lists.newArrayList(
            new KilledTasks(),
            new GCTime(),
            //            new NearMemoryLimit(),
            new CPUUsage(),
            new OutputPerMapTask()
        ),
        db.get()
    );

    Set<String> testEmailVictims = Sets.newHashSet();

    ExecutionAlerter testing = new ExecutionAlerter(
        new TestRecipientGenerator(
            AlertsHandlers.builder(TeamList.DEV_TOOLS)
                .setEngineeringRecipient(AlertRecipients.of(testEmailVictims))
                .build()),
        Lists.newArrayList(
        ),
        Lists.newArrayList(
            new ShortMaps(),
            new ShortReduces()
        ),
        db.get()
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
