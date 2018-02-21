package com.liveramp.workflow_monitor;


import com.google.common.collect.Lists;
import org.apache.log4j.Level;

import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
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

    //  alert every time this happens
    ExecutionAlerter spammyProduction = new ExecutionAlerter(
        new FromPersistenceGenerator(db.get()),
        Lists.newArrayList(
            new DiedUnclean()
        ),
        Lists.newArrayList(),
        db.get(),
        Integer.MAX_VALUE
    );

    //  generate alerts but send emails but only if the app runs fewer than 50 times a day
    ExecutionAlerter filteredProduction = new ExecutionAlerter(
        new FromPersistenceGenerator(db.get()),
        Lists.newArrayList(),
        Lists.newArrayList(
            new KilledTasks(),
            new GCTime(),
            new CPUUsage(),
            new OutputPerMapTask()
        ),
        db.get(),
        50
    );

    //  generate alerts but never email about it
    ExecutionAlerter quietProduction = new ExecutionAlerter(
        new TestRecipientGenerator(
            AlertsHandlers.builder(TeamList.NULL).build()),
        Lists.newArrayList(
        ),
        Lists.newArrayList(
            new ShortMaps(),
            new ShortReduces()
        ),
        db.get(),
        0
    );

    WorkflowMonitor monitor = new WorkflowMonitor(
        Lists.newArrayList(
            spammyProduction,
            filteredProduction,
            quietProduction
        )
    );

    monitor.monitor();

  }

}
