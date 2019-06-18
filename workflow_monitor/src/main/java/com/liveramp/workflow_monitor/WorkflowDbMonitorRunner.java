package com.liveramp.workflow_monitor;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

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
  private static final String WORKFLOW_MONITOR_PROPERTIES = "workflow.monitor.properties";
  private static final String WORKFLOW_MONITOR_ENV = "WORKFLOW_MONITOR_PROPERTIES";


  public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
    String configFile = Optional.ofNullable(System.getProperty(WORKFLOW_MONITOR_PROPERTIES))
        .orElseGet(() -> System.getenv(WORKFLOW_MONITOR_ENV));
    Properties properties = new Properties();

    if(!new File(configFile).exists()){
      throw new IllegalArgumentException("Please specify either "+WORKFLOW_MONITOR_PROPERTIES+" as a property or " +
          WORKFLOW_MONITOR_ENV+" as an environment variable.");
    }

    properties.load(new FileInputStream(configFile));

    String alertSourceList = properties.getProperty("alert_source_list");
    String alertSourceDomain = properties.getProperty("alert_source_domain");
    String mailHost = properties.getProperty("alert_mail_server");
    String uiServer = properties.getProperty("workflow_ui_server");

    ThreadLocal<IDatabases> db = new ThreadLocalWorkflowDb();

    //  alert every time this happens
    ExecutionAlerter spammyProduction = new ExecutionAlerter(
        new EmailFromPersistenceGenerator(db.get(), alertSourceList, alertSourceDomain, mailHost),
        Lists.newArrayList(
            DiedUnclean.create(properties)
        ),
        Lists.newArrayList(),
        db.get(),
        uiServer,
        Integer.MAX_VALUE
    );

    //  generate alerts but send emails but only if the app runs fewer than 50 times a day
    ExecutionAlerter filteredProduction = new ExecutionAlerter(
        new EmailFromPersistenceGenerator(db.get(), alertSourceList, alertSourceDomain, mailHost),
        Lists.newArrayList(),
        Lists.newArrayList(
            KilledTasks.create(properties),
            GCTime.create(properties),
            CPUUsage.create(properties),
            OutputPerMapTask.create(properties)
        ),
        db.get(),
        uiServer,
        50
    );

    //  generate alerts but never email about it
    ExecutionAlerter quietProduction = new ExecutionAlerter(
        new TestRecipientGenerator(
            new AlertsHandler.NoOp()),
        Lists.newArrayList(
        ),
        Lists.newArrayList(
            ShortMaps.create(properties),
            ShortReduces.create(properties),
            InputPerReduceTask.create(properties)
        ),
        db.get(),
        uiServer,
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
