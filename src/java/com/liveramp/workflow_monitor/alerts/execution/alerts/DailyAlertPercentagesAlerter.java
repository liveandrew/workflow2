package com.liveramp.workflow_monitor.alerts.execution.alerts;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hp.gagawa.java.elements.A;
import com.hp.gagawa.java.elements.Body;
import com.hp.gagawa.java.elements.Span;
import com.hp.gagawa.java.elements.Table;
import com.hp.gagawa.java.elements.Td;
import com.hp.gagawa.java.elements.Th;
import com.hp.gagawa.java.elements.Tr;

import com.liveramp.commons.collections.nested_map.ThreeKeyTuple;
import com.liveramp.commons.collections.nested_map.ThreeNestedCountingMap;
import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.databases.workflow_db.models.MapreduceCounter;
import com.liveramp.databases.workflow_db.models.MapreduceJob;
import com.liveramp.databases.workflow_db.models.StepAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowAlert;
import com.liveramp.databases.workflow_db.models.WorkflowAlertMapreduceJob;
import com.liveramp.databases.workflow_db.models.WorkflowAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.TeamList;
import com.rapleaf.jack.queries.Index;
import com.rapleaf.jack.queries.IndexHints;
import com.rapleaf.jack.queries.QueryOrder;
import com.rapleaf.jack.queries.Record;
import com.rapleaf.jack.queries.Records;

import static com.liveramp.workflow_core.WorkflowConstants.WORKFLOW_ALERT_RECOMMENDATIONS;
import static com.liveramp.workflow_core.WorkflowConstants.WORKFLOW_ALERT_SHORT_DESCRIPTIONS;
import static com.rapleaf.jack.queries.AggregatedColumn.COUNT;
import static com.rapleaf.jack.queries.AggregatedColumn.SUM;

public class DailyAlertPercentagesAlerter {

  private IDatabases db;
  private Integer hours = 24;
  private static Double ALERT_PERCENTAGE_THRESHOLD = .4;
  private static Long MIN_CLUSTER_TIME = Duration.ofMinutes(20).toMillis();
  private static String LINK_START = "http://workflows.liveramp.net/application.html?name=";
  private static String TABLE_STYLE = "min-width: 9em; padding-right: 1em; border-right: 1px dotted black;";

  private DailyAlertPercentagesAlerter(IDatabases db) {
    this.db = db;
  }

  private void generateAlerts() {
    db.getWorkflowDb().disableCaching();
    long threshold = System.currentTimeMillis() - Duration.ofHours(hours).toMillis();

    AlertsHandler alertsHandler = AlertsHandlers.builder(TeamList.DEV_TOOLS).build();

    try {
      Records mapreduceCounts = db.getWorkflowDb().createQuery()
          .from(MapreduceJob.TBL)
          .leftJoin(WorkflowAlertMapreduceJob.TBL)
          .on(WorkflowAlertMapreduceJob.MAPREDUCE_JOB_ID.equalTo(MapreduceJob.ID))
          .leftJoin(StepAttempt.TBL)
          .on(StepAttempt.ID.equalTo(MapreduceJob.STEP_ATTEMPT_ID.as(Long.class)))
          .leftJoin(WorkflowAttempt.TBL)
          .on(WorkflowAttempt.ID.equalTo(StepAttempt.WORKFLOW_ATTEMPT_ID.as(Long.class)))
          .leftJoin(WorkflowExecution.TBL)
          .on(WorkflowExecution.ID.equalTo(WorkflowAttempt.WORKFLOW_EXECUTION_ID.as(Long.class)))
          .where(WorkflowExecution.START_TIME.greaterThan(threshold))
          .select(WorkflowExecution.NAME, COUNT(MapreduceJob.ID))
          .groupBy(WorkflowExecution.NAME)
          .fetch();

      Map<String, Integer> totalMapreduceCounts = new HashMap<>();
      for (Record r : mapreduceCounts) {
        totalMapreduceCounts.put(r.getString(WorkflowExecution.NAME), r.getInt(COUNT(MapreduceJob.ID)));
      }

      Records minMapreduceDurationQuery = db.getWorkflowDb().createQuery().from(WorkflowExecution.TBL)
          .innerJoin(WorkflowAttempt.TBL)
          .on(WorkflowAttempt.WORKFLOW_EXECUTION_ID.equalTo(WorkflowExecution.ID.as(Integer.class)))
          .innerJoin(StepAttempt.TBL.with(IndexHints.force(Index.of("index_step_attempts_on_end_time"))))
          .on(StepAttempt.WORKFLOW_ATTEMPT_ID.equalTo(WorkflowAttempt.ID.as(Integer.class)))
          .innerJoin(MapreduceJob.TBL)
          .on(MapreduceJob.STEP_ATTEMPT_ID.equalTo(StepAttempt.ID))
          .innerJoin(MapreduceCounter.TBL)
          .on(MapreduceCounter.MAPREDUCE_JOB_ID.equalTo(MapreduceJob.ID.as(Integer.class)))

          .where(StepAttempt.END_TIME.greaterThan(threshold),
              MapreduceCounter.GROUP.in("YarnStats"),
              MapreduceCounter.NAME.in("VCORES_SECONDS", "MB_SECONDS"))
          .groupBy(WorkflowExecution.NAME)
          .select(WorkflowExecution.NAME, SUM(MapreduceCounter.VALUE))
          .orderBy(SUM(MapreduceCounter.VALUE), QueryOrder.DESC)
          .fetch();

      // Eventually get min since query is sorted in descending order
      Map<String, Long> minMapreduceDurations = new HashMap<>();
      for (Record r : minMapreduceDurationQuery) {
        minMapreduceDurations.put(r.get(WorkflowExecution.NAME), r.get(SUM(MapreduceCounter.VALUE)));
      }

      Records badMapreduceJobs = db.getWorkflowDb().createQuery()
          .from(MapreduceJob.TBL)
          .innerJoin(WorkflowAlertMapreduceJob.TBL)
          .on(WorkflowAlertMapreduceJob.MAPREDUCE_JOB_ID.equalTo(MapreduceJob.ID))
          .innerJoin(WorkflowAlert.TBL)
          .on(WorkflowAlert.ID.equalTo(WorkflowAlertMapreduceJob.WORKFLOW_ALERT_ID))
          .innerJoin(StepAttempt.TBL)
          .on(StepAttempt.ID.equalTo(MapreduceJob.STEP_ATTEMPT_ID.as(Long.class)))
          .innerJoin(WorkflowAttempt.TBL)
          .on(WorkflowAttempt.ID.equalTo(StepAttempt.WORKFLOW_ATTEMPT_ID.as(Long.class)))
          .innerJoin(WorkflowExecution.TBL)
          .on(WorkflowExecution.ID.equalTo(WorkflowAttempt.WORKFLOW_EXECUTION_ID.as(Long.class)))
          .where(
              WorkflowExecution.START_TIME.greaterThan(threshold),
              WorkflowAlert.ALERT_CLASS.notEqualTo(DiedUnclean.class.getName()))
          .select(COUNT(WorkflowAlertMapreduceJob.ID), WorkflowExecution.NAME,
              WorkflowAlert.ALERT_CLASS, StepAttempt.STEP_TOKEN, WorkflowAttempt.ERROR_EMAIL)
          .groupBy(WorkflowExecution.NAME, WorkflowAlert.ALERT_CLASS, StepAttempt.STEP_TOKEN, WorkflowAttempt.ERROR_EMAIL)
          .fetch();

      ThreeNestedCountingMap<String, String, String> mapreduceAlertCounts = new ThreeNestedCountingMap(0L);

      Map<ThreeKeyTuple<String, String, String>, String> mapreduceErrorEmails = new HashMap<>();
      for (Record r : badMapreduceJobs) {
        String name = r.getString(WorkflowExecution.NAME);
        String alert = r.getString(WorkflowAlert.ALERT_CLASS).replaceAll(".*\\.", "");
        String step = r.getString(StepAttempt.STEP_TOKEN).replaceAll("[0-9]", "");
        mapreduceAlertCounts.incrementAndGet(name, alert, step, r.get(COUNT(WorkflowAlertMapreduceJob.ID)).longValue());
        mapreduceErrorEmails.put(new ThreeKeyTuple<>(name, alert, step), r.get(WorkflowAttempt.ERROR_EMAIL));
      }

      Map<String, Set<ThreeKeyTuple<String, String, String>>> emailAlerts = new HashMap<>();
      for (ThreeKeyTuple<String, String, String> nameAlertStep : mapreduceAlertCounts.key123Set()) {
        String appName = nameAlertStep.getK1();
        Integer totalMapreduceCount = totalMapreduceCounts.get(appName);
        Long minClusterTime = minMapreduceDurations.get(appName);
        Double alertsPerMRJob = mapreduceAlertCounts.get(nameAlertStep) / totalMapreduceCount.doubleValue();
        String email = mapreduceErrorEmails.get(nameAlertStep);

        if (alertsPerMRJob > ALERT_PERCENTAGE_THRESHOLD && minClusterTime != null && minClusterTime > MIN_CLUSTER_TIME) {
          Set s = emailAlerts.getOrDefault(email, new HashSet<>());
          s.add(nameAlertStep);
          if (email == null) {
            email = "dev-null@liveramp.com";
          }
          emailAlerts.put(email, s);
        }
      }

      ArrayList<ThreeKeyTuple<ThreeKeyTuple<String, String, String>, Double, String>> percentages = new ArrayList<>();
      for (String email : emailAlerts.keySet()) {
        Table table = new Table();
        table.appendChild(new Tr().appendChild(
            new Th().appendText("Name").setStyle(TABLE_STYLE),
            new Th().appendText("Alert").setStyle(TABLE_STYLE),
            new Th().appendText("Step").setStyle(TABLE_STYLE),
            new Th().appendText("Alerts per MR job").setStyle(TABLE_STYLE)
        ));

        Set<String> alertClasses = new HashSet<>();
        for (ThreeKeyTuple<String, String, String> nameAlertStep : emailAlerts.get(email)) {
          Double percent = totalMapreduceCounts.get(nameAlertStep.getK1()) / mapreduceAlertCounts.get(nameAlertStep).doubleValue();
          percentages.add(new ThreeKeyTuple<>(nameAlertStep, percent, email));

          table.appendChild(
              new Tr()
                  .appendChild(
                      new Td()
                          .appendChild(
                              new A().appendChild(new Span().appendText(nameAlertStep.getK1()))
                                  .setHref(LINK_START + nameAlertStep.getK1())
                          ).setStyle(TABLE_STYLE),
                      new Td().appendText(nameAlertStep.getK2()).setStyle(TABLE_STYLE),
                      new Td().appendText(nameAlertStep.getK3()).setStyle(TABLE_STYLE),
                      new Td().appendText(new DecimalFormat("#.##").format(percent)).setStyle(TABLE_STYLE)
                  ));
          alertClasses.add(nameAlertStep.getK2());
        }
        String message = "The following workflows have a high MapReduce job error rate over the past " + hours + " hours:\n\n"
            + new Body().appendChild(table).write();

        for (String alertClass : alertClasses) {
          message += "<b>" + alertClass + "</b>: " + WORKFLOW_ALERT_SHORT_DESCRIPTIONS.get(alertClass) +
              " " + WORKFLOW_ALERT_RECOMMENDATIONS.get(alertClass) + "\n";
        }

        alertsHandler.sendAlert("[DT] Workflows have high error rates",
            message,
            //    AlertRecipients.of(email));
            AlertRecipients.of("kong@liveramp.com"));
      }
      percentages.sort(
          (ThreeKeyTuple<ThreeKeyTuple<String, String, String>, Double, String> o1, ThreeKeyTuple<ThreeKeyTuple<String, String, String>, Double, String> o2) ->
              o1.getK2() < o2.getK2() ? 1 : -1
      );

      final Table table = new Table();
      table.appendChild(new Tr().appendChild(
          new Th().appendText("name"),
          new Th().appendText("alert"),
          new Th().appendText("step"),
          new Th().appendText("recipient"),
          new Th().appendText("min cluster time"),
          new Th().appendText("errors/job")
      ));
      //<Name, Alert, Step>, Error %, Email
      for (ThreeKeyTuple<ThreeKeyTuple<String, String, String>, Double, String> d : percentages) {
        ThreeKeyTuple<String, String, String> nameAlertStep = d.getK1();
        table.appendChild(
            new Tr()
                .appendChild(
                    new Td()
                        .appendChild(
                            new A().appendChild(new Span().appendText(nameAlertStep.getK1()))
                                .setHref(LINK_START + nameAlertStep.getK1())
                        ),
                    new Td().appendText(nameAlertStep.getK2()),
                    new Td().appendText(nameAlertStep.getK3()),
                    new Td().appendText(d.getK3()),
                    new Td().appendText(new DecimalFormat("####E0").format(minMapreduceDurations.get(nameAlertStep.getK1()))),
                    new Td().appendText(new DecimalFormat("#.##").format(d.getK2()))
                )

        );
      }
      final Body body = new Body();
      body.appendChild(table);
      alertsHandler.sendAlert("[DT] Workflow alerts summary", body.write(), AlertRecipients.of("kong@liveramp.com", "bpodgursky@liveramp.com"));

    } catch (IOException e) {
      alertsHandler.sendAlert("[DT] Daily Workflow alerts summary failed!", e.toString(), AlertRecipients.of("kong@liveramp.com"));
    }
    return;
  }

  public static void main(String[] args) throws Exception {
    new DailyAlertPercentagesAlerter(new DatabasesImpl()).generateAlerts();
  }
}
