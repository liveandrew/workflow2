package com.liveramp.workflow_ui.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.nested_map.ThreeNestedCountingMap;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.databases.workflow_db.DatabasesImpl;
import com.liveramp.databases.workflow_db.IDatabases;
import com.liveramp.databases.workflow_db.IWorkflowDb;
import com.liveramp.databases.workflow_db.models.Application;
import com.liveramp.databases.workflow_db.models.ApplicationCounterSummary;
import com.liveramp.databases.workflow_db.models.Dashboard;
import com.liveramp.databases.workflow_db.models.DashboardApplication;
import com.liveramp.databases.workflow_db.models.WorkflowAttempt;
import com.liveramp.databases.workflow_db.models.WorkflowExecution;
import com.liveramp.workflow.types.WorkflowAttemptStatus;
import com.liveramp.workflow.types.WorkflowExecutionStatus;
import com.liveramp.workflow_db_state.WorkflowQueries;
import com.rapleaf.jack.queries.GenericConstraint;
import com.rapleaf.jack.queries.GenericQuery;
import com.rapleaf.jack.queries.Index;
import com.rapleaf.jack.queries.IndexHint;
import com.rapleaf.jack.queries.IndexHints;
import com.rapleaf.jack.queries.Record;
import com.rapleaf.jack.queries.Records;
import com.rapleaf.jack.queries.Table;
import com.rapleaf.jack.queries.where_operators.In;

import static com.rapleaf.jack.queries.AggregatedColumn.COUNT;


public class QueryUtil {
  private static final Logger LOG = LoggerFactory.getLogger(QueryUtil.class);

  public static JSONObject getDashboardStatus(IWorkflowDb rldb, long endStartTime, String dashboardName) throws IOException, JSONException {

    //  query 1: anything NOT RUNNING.  workflow attempt end time greater than end time
    JSONObject endedStatuses = getStatuses(rldb,
        dashboardName,
        WorkflowExecution.TBL,
        WorkflowAttempt.TBL.with(IndexHints.force(Index.of("index_workflow_attempts_on_end_time"))),
        WorkflowAttempt.END_TIME.greaterThan(endStartTime),
        WorkflowAttempt.STATUS.notEqualTo(WorkflowAttemptStatus.RUNNING.getValue())
    );

    //  query 2: anything RUNNING, workflow execution start time greater than end time
    JSONObject statuses = getStatuses(rldb,
        dashboardName,
        WorkflowExecution.TBL.with(IndexHints.force(Index.of("start_time_idx"))),
        WorkflowAttempt.TBL,
        WorkflowExecution.START_TIME.greaterThan(endStartTime),
        WorkflowAttempt.STATUS.equalTo(WorkflowAttemptStatus.RUNNING.getValue())
    );

    //  no overlap, can combine.
    Iterator keys = endedStatuses.keys();
    while(keys.hasNext()){
      String key = keys.next().toString();
      statuses.put(key, endedStatuses.getInt(key));
    }

    //  avoid nils in the javascript
    for (WorkflowAttemptStatus status : WorkflowAttemptStatus.values()) {
      if (!statuses.has(status.toString())) {
        statuses.put(status.toString(), 0);
      }
    }

    return statuses;

  }

  private static JSONObject getStatuses(IWorkflowDb rldb,
                                        String dashboardName,
                                        Table workflowExecutions,
                                        Table workflowAttempts,
                                        GenericConstraint constraint,
                                        GenericConstraint... constraintList) throws IOException, JSONException {

    JSONObject statuses = new JSONObject();
    for (Record record : rldb.createQuery().from(Dashboard.TBL)
        .where(Dashboard.NAME.equalTo(dashboardName))
        .innerJoin(DashboardApplication.TBL)
        .on(DashboardApplication.DASHBOARD_ID.equalTo(Dashboard.ID.as(Integer.class)))
        .innerJoin(workflowExecutions)
        .on(WorkflowExecution.APPLICATION_ID.equalTo(DashboardApplication.APPLICATION_ID))
        .innerJoin(workflowAttempts)
        .on(WorkflowAttempt.WORKFLOW_EXECUTION_ID.equalTo(WorkflowExecution.ID.as(Integer.class)))
        .where(constraint, constraintList)
        .groupBy(WorkflowAttempt.STATUS)
        .select(WorkflowAttempt.STATUS, COUNT(WorkflowAttempt.ID))
        .fetch()) {

      statuses.put(WorkflowAttemptStatus.findByValue(record.getInt(WorkflowAttempt.STATUS)).toString(), record.getInt(COUNT(WorkflowAttempt.ID)));
    }
    return statuses;
  }

  public static JSONArray getDashboardConfigurations(GenericQuery query) throws IOException, JSONException {

    // fetch first since it'll get mutated
    Records allDashboards = query.select(Dashboard.TBL.getAllColumns()).fetch();

    Multimap<String, String> appsByDashboard = HashMultimap.create();
    for (Record record : WorkflowQueries.joinDashboardsOnApplication(query).select(Dashboard.NAME, Application.NAME).fetch()) {
      appsByDashboard.put(record.get(Dashboard.NAME), record.get(Application.NAME));
    }

    JSONArray dashboardInfo = new JSONArray();
    for (Record record : allDashboards) {
      String dashName = record.get(Dashboard.NAME);
      dashboardInfo.put(new JSONObject()
          .put("name", dashName)
          .put("applications", new JSONArray(appsByDashboard.get(dashName))));
    }

    return dashboardInfo;
  }


  public static JSONObject getCountersForApp(IWorkflowDb rldb,
                                             String appName,
                                             Multimap<String, String> countersToQuery,
                                             LocalDate startDate,
                                             LocalDate endDate,
                                             ApplicationStatistic additionalStatistics) throws IOException, SQLException, JSONException {

    List<ApplicationCounterSummary> summaries = computeSummaries(rldb, appName, countersToQuery, startDate, endDate);


    ThreeNestedMap<Long, String, String, Long> counterMap = new ThreeNestedCountingMap<>(0L);

    for (ApplicationCounterSummary summary : summaries) {
      counterMap.put(summary.getDate(), summary.getGroup(), summary.getName(), summary.getValue());
    }

    JSONObject dateMap = new JSONObject();

    for (Long date : counterMap.key1Set()) {
      TwoNestedMap<String, String, Long> dateCounters = counterMap.get(date);
      JSONObject additionalStats = additionalStatistics.deriveStats(dateCounters);

      JSONObject counters = new JSONObject();
      for (TwoNestedMap.Entry<String, String, Long> entry : dateCounters.entrySet()) {
        counters.put(entry.getK1() + "." + entry.getK2(), entry.getValue());
      }

      JSONObject appObj = new JSONObject()
          .put("counters", counters);

      Iterator iter = additionalStats.keys();
      while (iter.hasNext()) {
        String key = iter.next().toString();
        appObj.put(key, additionalStats.get(key));
      }

      dateMap.put(Long.toString(date), appObj);
    }

    return dateMap;

  }


  public static JSONObject getCountersPerApplication(IWorkflowDb rldb, Multimap<String, String> countersToQuery, ApplicationStatistic additionalStatistics, LocalDate startDate, LocalDate endDate) throws IOException, JSONException, SQLException {
    ThreeNestedCountingMap<String, String, String> applicationCounters = new ThreeNestedCountingMap<String, String, String>(0l);

    //  get summaries from startedAfter ... startedBefore

    List<ApplicationCounterSummary> summaries = computeSummaries(rldb, null, countersToQuery, startDate, endDate);

    //  only ~1k right now, is fine
    Map<Long, String> appToName = Maps.newHashMap();
    for (Application application : rldb.applications().findAll()) {
      appToName.put(application.getId(), application.getName());
    }

    for (ApplicationCounterSummary summary : summaries) {
      if (countersToQuery.containsEntry(summary.getGroup(), summary.getName())) {
        if (summary.getApplicationId() != null) {
          applicationCounters.incrementAndGet(
              appToName.get(summary.getApplicationId().longValue()),
              summary.getGroup(),
              summary.getName(),
              summary.getValue()
          );
        } else {
          applicationCounters.incrementAndGet(
              "CLUSTER_TOTAL",
              summary.getGroup(),
              summary.getName(),
              summary.getValue()
          );
        }
      }
    }

    Map<String, Long> appCounts = Maps.newHashMap();
    appCounts.put("CLUSTER_TOTAL", 0L);

    for (Record record : WorkflowQueries.getExecutionsByEndQuery(rldb, startDate, endDate).fetch()) {
      String workflowName = record.getString(WorkflowExecution.NAME);

      if (workflowName != null) {
        long count = (long)record.get(COUNT(WorkflowExecution.ID));
        appCounts.put(workflowName, count);
        appCounts.put("CLUSTER_TOTAL", appCounts.get("CLUSTER_TOTAL") + count);
      }
    }


    JSONObject appMap = new JSONObject();
    for (String appName : Iterables.concat(appCounts.keySet(), Lists.newArrayList("CLUSTER_TOTAL"))) {

      JSONObject counters = new JSONObject();

      TwoNestedMap<String, String, Long> appCounters = applicationCounters.get(appName);
      for (TwoNestedMap.Entry<String, String, Long> entry : appCounters.entrySet()) {
        counters.put(entry.getK1() + "." + entry.getK2(), entry.getValue());
      }

      JSONObject additionalStats = additionalStatistics.deriveStats(appCounters);
      Iterator iter = additionalStats.keys();

      JSONObject appObj = new JSONObject()
          .put("counters", counters);

      while (iter.hasNext()) {
        String key = iter.next().toString();
        appObj.put(key, additionalStats.get(key));
      }

      appMap.put(appName, appObj
          .put("count", appCounts.get(appName))
      );

    }

    return appMap;


  }

  public static List<ApplicationCounterSummary> computeSummaries(IWorkflowDb rldb,
                                                                  String appName,
                                                                  Multimap<String, String> countersToQuery,
                                                                  LocalDate startDate, LocalDate endDate) throws IOException, SQLException {
    int days = Days.daysBetween(startDate, endDate).getDays();
    Summarizer.summarizeApplicationCounters(countersToQuery, rldb, days, endDate);
    return WorkflowQueries.getSummaries(rldb, appName, countersToQuery, startDate, endDate);

  }

  public static Multimap<WorkflowExecution, WorkflowAttempt> queryWorkflowExecutions(IDatabases rldb,
                                                                                     Map<String, String> parameters,
                                                                                     Integer limit) throws IOException {

    return WorkflowQueries.getExecutionsToAttempts(rldb,
        safeLong(parameters.get("id")),
        parameters.get("dashboard"),
        parameters.get("name"),
        parameters.get("scope_identifier"),
        parameters.get("host"),
        safeInt(parameters.get("app_type")),
        safeLong(parameters.get("started_after")),
        safeLong(parameters.get("started_before")),
        safeStatus(parameters.get("status")),
        limit);
  }

  public static Integer safeInt(String val) {
    if (val != null) {
      return Integer.parseInt(val);
    }
    return null;
  }

  public static Long safeLong(String val) {
    if (val != null) {
      return Long.parseLong(val);
    }
    return null;
  }

  public static WorkflowExecutionStatus safeStatus(String val) {
    if (val != null) {
      return WorkflowExecutionStatus.valueOf(val);
    }
    return null;
  }


  public static void main(String[] args) throws IOException, JSONException {

    IWorkflowDb db = new DatabasesImpl().getWorkflowDb();

    JSONObject status = getDashboardStatus(db, DateTime.now().minusHours(1).getMillis(), args[0]);
    System.out.println(status);

  }

}