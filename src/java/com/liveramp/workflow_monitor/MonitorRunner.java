package com.liveramp.workflow_monitor;

import com.google.common.collect.Lists;

import com.liveramp.java_support.logging.LoggingHelper;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlertGenerator;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlerter;
import com.liveramp.workflow_monitor.alerts.execution.alerts.DiedUnclean;
import com.liveramp.workflow_monitor.alerts.execution.alerts.KilledTasks;
import com.liveramp.workflow_monitor.alerts.execution.recipient.FromAttemptGenerator;
import com.liveramp.workflow_monitor.alerts.execution.recipient.TestRecipientGenerator;
import com.rapleaf.db_schemas.DatabasesImpl;
import com.rapleaf.db_schemas.IDatabases;

public class MonitorRunner {

  public static void main(String[] args) throws InterruptedException {
    LoggingHelper.setLoggingProperties(MonitorRunner.class.getSimpleName());

    IDatabases db = new DatabasesImpl();
    db.getRlDb().disableCaching();

    ExecutionAlerter production = new ExecutionAlerter(
        new FromAttemptGenerator(),
        Lists.<ExecutionAlertGenerator>newArrayList(
            new DiedUnclean()
        ),
        db
    );

    ExecutionAlerter testing = new ExecutionAlerter(
        new TestRecipientGenerator("bpodgursky@liveramp.com"),
        Lists.<ExecutionAlertGenerator>newArrayList(
            new DiedUnclean(),
            new KilledTasks()
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
