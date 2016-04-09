package com.liveramp.workflow_monitor;

import com.google.common.collect.Lists;

import com.liveramp.java_support.logging.LoggingHelper;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlerter;
import com.liveramp.workflow_monitor.alerts.execution.MapreduceJobAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.alerts.CPUUsage;
import com.liveramp.workflow_monitor.alerts.execution.alerts.DiedUnclean;
import com.liveramp.workflow_monitor.alerts.execution.alerts.GCTime;
import com.liveramp.workflow_monitor.alerts.execution.alerts.KilledTasks;
import com.liveramp.workflow_monitor.alerts.execution.alerts.NearMemoryLimit;
import com.liveramp.workflow_monitor.alerts.execution.alerts.OutputPerMapTask;
import com.liveramp.workflow_monitor.alerts.execution.recipient.FromPersistenceGenerator;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.IDatabases;

public class MonitorRunner {

  public static void main(String[] args) throws InterruptedException {
    LoggingHelper.setLoggingProperties(MonitorRunner.class.getSimpleName());

    IDatabases db = new DatabasesImpl();
    db.getRlDb().disableCaching();

    ExecutionAlerter production = new ExecutionAlerter(
        new FromPersistenceGenerator(db),
        Lists.<ExecutionAlertGenerator>newArrayList(
            new DiedUnclean()
        ),
        Lists.<MapreduceJobAlertGenerator>newArrayList(
            new KilledTasks(),
            new GCTime(),
            new NearMemoryLimit(),
            new CPUUsage(),
            new OutputPerMapTask()
        ),
        db
    );

//    ExecutionAlerter testing = new ExecutionAlerter(
//        new TestRecipientGenerator(
//            AlertsHandlers.builder(TeamList.DEV_TOOLS)
//                .setEngineeringRecipient(AlertRecipients.of("bpodgursky+alert-firehose@liveramp.com"))
//                .build()),
//        Lists.<ExecutionAlertGenerator>newArrayList(
//            new DiedUnclean()
//        ),
//        Lists.<MapreduceJobAlertGenerator>newArrayList(
//            new KilledTasks(),
//            new GCTime(),
//            new NearMemoryLimit(),
//            new CPUUsage(),
//            new OutputPerMapTask()
//        ),
//        db
//    );


    WorkflowMonitor monitor = new WorkflowMonitor(
        Lists.newArrayList(
            production
        )
    );

    monitor.monitor();

  }

}
