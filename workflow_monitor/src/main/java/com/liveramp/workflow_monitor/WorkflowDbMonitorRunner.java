package com.liveramp.workflow_monitor;


import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.workflow_db_state.ThreadLocalWorkflowDb;
import com.liveramp.workflow_monitor.alerts.execution.ExecutionAlerter;
import com.liveramp.workflow_monitor.alerts.execution.alerts.CPUUsage;
import com.liveramp.workflow_monitor.alerts.execution.alerts.DiedUnclean;
import com.liveramp.workflow_monitor.alerts.execution.alerts.GCTime;
import com.liveramp.workflow_monitor.alerts.execution.alerts.InputPerReduceTask;
import com.liveramp.workflow_monitor.alerts.execution.alerts.KilledTasks;
import com.liveramp.workflow_monitor.alerts.execution.alerts.OutputPerMapTask;
import com.liveramp.workflow_monitor.alerts.execution.alerts.ShortMaps;
import com.liveramp.workflow_monitor.alerts.execution.alerts.ShortReduces;
import com.liveramp.workflow_monitor.alerts.execution.recipient.EmailFromPersistenceGenerator;
import com.liveramp.workflow_monitor.alerts.execution.recipient.TestRecipientGenerator;

public class WorkflowDbMonitorRunner {

  public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {

    String configFile = args[0];

    Map<String, Object> config = new Gson().fromJson(
        new FileReader(configFile), new TypeToken<HashMap<String, Object>>() {
        }.getType()
    );

    String alertSourceList = (String)config.get("alert_source_list");
    String alertSourceDomain = (String)config.get("alert_source_domain");
    String mailHost = (String)config.get("alert_mail_server");

    ThreadLocal<IDatabases> db = new ThreadLocalWorkflowDb();

    //  alert every time this happens
    ExecutionAlerter spammyProduction = new ExecutionAlerter(
        new EmailFromPersistenceGenerator(db.get(), alertSourceList, alertSourceDomain, mailHost),
        Lists.newArrayList(
            new DiedUnclean()
        ),
        Lists.newArrayList(),
        db.get(),
        Integer.MAX_VALUE
    );

    //  generate alerts but send emails but only if the app runs fewer than 50 times a day
    ExecutionAlerter filteredProduction = new ExecutionAlerter(
        new EmailFromPersistenceGenerator(db.get(), alertSourceList, alertSourceDomain, mailHost),
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
            new AlertsHandler.NoOp()),
        Lists.newArrayList(
        ),
        Lists.newArrayList(
            new ShortMaps(),
            new ShortReduces(),
            new InputPerReduceTask()
        ),
        db.get(),
        0
    );

    WorkflowMonitor.monitor(Lists.newArrayList(
        spammyProduction,
        filteredProduction,
        quietProduction
        )
    );

  }

}
